from flask import abort, jsonify
import os
import base64, hashlib, hmac
import google.generativeai as genai
import json
from datetime import datetime, date
import pytz
import requests
from typing import Optional

from linebot import (
    LineBotApi, WebhookParser
)
from linebot.exceptions import (
    InvalidSignatureError
)
from linebot.models import (
    MessageEvent, TextMessage, TextSendMessage, QuickReply, QuickReplyButton, PostbackAction, PostbackEvent, MessageAction
)

from models import UserStatus, Diary, Question, Options
import querys


'''
環境変数
'''
# CHANNEL_ACCESS_TOKEN
channel_access_token = os.environ.get('LINE_CHANNEL_ACCESS_TOKEN')
# CANNEL_SECRET
channel_secret = os.environ.get('LINE_CHANNEL_SECRET')
# GEMINI_APIKEY
gemini_api_key = os.environ.get('GEMINI_API_KEY')
# 日記作成のシステムプロンプト
system_prompt_diary = f"""
与えられた文章から日記を生成し、JSON形式で答えてください。
その際、以下の条件に沿って回答してください。
1. 与えられた文章に場面描写や心情、セリフを追加して、最大300文字以内の短い物語にしてください。登場人物のキャラクター設定は以下に従ってください。
    ・主体となる人物は多少皮肉っぽい、演劇的な言葉選びや言い回しにしてください
    ・その他の人物は以下の例の中からランダムに性格付けしてください
    例：・温和でお人よし、主体となる人物の皮肉っぽい言い回しをたしなめる
        ・陽気でそそっかしい、主体となる人物を振り回す
        ・謎めいて陰を感じさせ、含みのある物言いをする
        ・高圧的でプライドが高く、主体となる人物を下に見ている
        ・内向的で卑屈なところがあり口数が少ない
        ・華やかで気品があり、人の目を引く振る舞いをする
2. 次に、物語を英文にしてください。英文作成の際、探偵小説のような文体にしてください。
3. 上記2.の英文をさらに和訳してください。
4. 英文をもとに、3問の3択問題を作成してください。
    ・問題文と選択肢を英語で作成してください。
    ・正答番号を指定してください。選択肢の1番目が正解なら1、2番目が正解なら2...というようにしてください。
    ・正解解説を日本語で作成してください
5. JSON形式で以下の通り項目を設定してください。
    original : 2.で作成した英文。
    translation : 3.で作成した訳文。
    exercises : 4.で作成した問題のリスト（3問）。内容項目はquestion_no 、question、options 、answer 、explanation 。
        question_no : 問題番号。1からカウントする。
        question : 4.で作成した問題文。
        options : 4.で作成した選択肢のリスト（3択）。内容項目はoption_no、option 。
            option_no : 選択肢番号。1からカウントする。
            option : 選択肢のテキスト。
        answer : 4.で作成した問題の正解。option_noに合わせる。
        explanation : 4.で作成した問題の日本語解説文。
"""
# 質問用のシステムプロンプト
system_prompt_asking = """
以下の英文について質問されるので、和訳の内容も踏まえて100字程度で回答してください。
英文：{english_text} 
和訳：{japanese_text}
"""

'''
メイン処理
'''
def main(request):
    # LINEBOTの設定
    line_bot_api = LineBotApi(channel_access_token)
    parser = WebhookParser(channel_secret)

    # シグネチャの確認
    body = request.get_data(as_text=True)
    hash = hmac.new(channel_secret.encode('utf-8'),
        body.encode('utf-8'), hashlib.sha256).digest()
    signature = base64.b64encode(hash).decode()

    if signature != request.headers['X_LINE_SIGNATURE']:
        return abort(405)

    try:
        events = parser.parse(body, signature)
    except InvalidSignatureError:
        return abort(405)

    for event in events:
        # 返信用変数準備
        reply_data = []
        # ローディングアニメーション
        url_loading = 'https://api.line.me/v2/bot/chat/loading/start'
        headers_loading = {
            'Content-Type': 'application/json',
            "Authorization": f'Bearer {channel_access_token}'
        }
        payload_loading = {
            "chatId": event.source.user_id,
            "loadingSeconds": 40
        }
        if isinstance(event, MessageEvent):
            # メッセージを受信した場合
            response = requests.post(url_loading, headers=headers_loading, data=json.dumps(payload_loading)) 
            if isinstance(event.message, TextMessage):
                # タイムゾーン設定
                timezone_japan = pytz.timezone('Asia/Tokyo')
                # 日本時間の現在時刻を取得
                now_japan = datetime.now(pytz.utc).astimezone(timezone_japan)
                # 日付のみに絞り込む
                today = now_japan.date()
                # テキストメッセージならユーザーステータステーブルを検索
                user_status = querys.select_user_status(event.source.user_id)
                if user_status is None:
                    # ユーザーステータスが存在しない場合、日記データを新規作成
                    diary_id = create_diary(event.source.user_id, event.message.text, today)
                    # 新規ユーザーステータスを編集
                    user_status = UserStatus(
                        user_id=event.source.user_id,
                        status='0',
                        current_diary_id=diary_id,
                        current_question_no=None,
                        latest_diary_date=today
                    )
                    # ユーザーステータスを新規作成
                    querys.insert_user_status(user_status)
                elif user_status.current_diary_id is None:
                    # 処理中の日記IDが未設定の場合
                    if user_status.latest_diary_date == today:
                        # 今日の日記が処理された後であればメッセージをそのままAIに送って応答を返す
                        response = generate_ai_message(event.message.text, "text/plain", None)
                        reply_data.append(TextSendMessage(text=response))
                        line_bot_api.reply_message(event.reply_token, reply_data)
                        return jsonify({ 'message': 'ok'})
                    else:
                        # 今日の日記が未作成の場合、日記データを新規作成
                        diary_id = create_diary(event.source.user_id, event.message.text, today)
                        # ユーザーステータスを編集
                        user_status.current_diary_id = diary_id
                        user_status.latest_diary_date = today

                # 日記データを検索
                diary = querys.select_diary(user_status.current_diary_id)
                if user_status.status == '1' :
                    # ステータスが出題中の場合、問題データを取得する
                    question = querys.select_question(user_status.current_diary_id, user_status.current_question_no)
                    # 受信したメッセージが正解かどうかを判定する
                    if querys.is_correct(question_id=question.id, option_no=int(event.message.text)):
                        # 正解の場合は日記データの正答数を更新する
                        diary.number_of_correct_answers += 1
                        querys.update_diary(diary)
                        # メッセージを追加
                        reply_data.append(TextSendMessage(text='正解です！'))
                    else:
                        # 不正解の場合は問題の誤答フラグを更新する
                        querys.update_question(question.id)
                        # メッセージを追加
                        reply_data.append(TextSendMessage(text='不正解です。'))
                    # メッセージに解説文を追加
                    reply_data.append(TextSendMessage(text=question.explanation_text))
                    if user_status.current_question_no == 3:
                        # 処理中の問題数が3の場合、出題終了としてユーザーステータスを変更
                        user_status.status = '0'
                        user_status.current_diary_id = None
                        user_status.current_question_no = None
                        # 成績発表メッセージを編集
                        reply_data.append(TextSendMessage(text=f'今日は3問中{diary.number_of_correct_answers}問正解しました！'))
                        # 和訳を編集
                        reply_data.append(TextSendMessage(text=diary.japanese_text))
                    else:
                        # 処理中の問題番号が3以外の場合、次の問題を作成
                        user_status.current_question_no += 1
                        reply_data.append(edit_question(diary.id, user_status.current_question_no))
                elif user_status.status == '2':
                    # 質問中の場合、AI応答を生成して応答を返す
                    response = generate_ai_message(event.message.text, "text/plain", 
                            system_prompt_asking.format(english_text=diary.english_text, 
                            japanese_text=diary.japanese_text))
                    response += '\n' + '（ほかに質問があれば続けてください）'
                    quick_Action = [QuickReplyButton(action=PostbackAction(label='問題を解く', data='try_to_answer',display_text='問題を解く'))]
                    reply_data.append(TextSendMessage(text=response,quick_reply=QuickReply(items=quick_Action)))
                else:
                    # 上記以外の場合、クイックリプライにPostbackアクションを入れたボタンを作る
                    quick_Action = [QuickReplyButton(action=PostbackAction(label='問題を解く', data='try_to_answer',display_text='問題を解く'))
                        ,QuickReplyButton(action=PostbackAction(label='質問する', data='ask_question',display_text='質問する'))]
                    # 日記の英文をメッセージに編集
                    reply_data.append(TextSendMessage(text=diary.english_text,quick_reply=QuickReply(items=quick_Action)))
                    
                # ユーザーステータスを更新
                querys.update_user_status(user_status)

                # 応答内容をLINEで送信
                line_bot_api.reply_message(event.reply_token, reply_data) 
            else:
                continue

        elif isinstance(event, PostbackEvent):
            # ポストバックイベントの場合、ユーザーステータスを取得
            user_status = querys.select_user_status(event.source.user_id)
            # 日記データを取得
            diary = querys.select_diary(user_status.current_diary_id)
            if event.postback.data == 'try_to_answer':
                # 問題を解く場合、1問目を出題
                # 処理中の問題番号を更新
                user_status.current_question_no = 1
                # 問題メッセージを編集
                reply_data.append(edit_question(diary.id, user_status.current_question_no))
                # ユーザーステータスを出題中にする
                user_status.status = '1'
            elif event.postback.data == 'ask_question':
                # 質問する場合、ユーザーステータスを質問中にする
                user_status.status = '2'
                # 返信メッセージを編集
                reply_data.append(TextSendMessage(text='質問をどうぞ。'))

            # ユーザーステータス更新
            querys.update_user_status(user_status)
            # 応答内容をLINEで送信
            line_bot_api.reply_message(event.reply_token, reply_data) 

    return jsonify({ 'message': 'ok'})


'''
日記・問題・選択肢にデータを追加する
'''
def create_diary(user_id: str, message_text: str, diary_date: date) -> int:
    # 日記用のデータをAIで生成する
    response = generate_ai_message(message_text, "application/json", system_prompt_diary)
    # 応答内容から各テーブルにレコードを追加する
    data = json.loads(response)
    # 日記テーブル追加
    diaryEntry = Diary(
        id=0,
        user_id=user_id,
        diary_date=diary_date,
        original_text=message_text,
        english_text=data['original'],
        japanese_text=data['translation'],
        number_of_correct_answers=0
    )
    diary_id = querys.insert_diary(diaryEntry)
    # 問題テーブル追加（AI応答のexercises内question）
    for exercise in data['exercises'] :
        questionEntry = Question(
            id=0,
            diary_id=diary_id,
            question_no=exercise['question_no'],
            question_text=exercise['question'],
            explanation_text=exercise['explanation'],
            mistake_flag=False
        )
        question_id = querys.insert_question(questionEntry)
        # 選択肢テーブル追加（AI応答のexercises内options）
        for option in exercise['options'] :
            correct_flag = exercise['answer'] == option['option_no']
            option = Options(
                id=0,
                question_id=question_id,
                option_no=option['option_no'],
                option_text=option['option'],
                correct_flag=correct_flag
            )
            querys.insert_option(option)
    # 作成した日記IDを返す
    return diary_id

'''
AIモデルの応答テキストを返す
パラメータがある場合はシステムパラメータに設定する
'''
def generate_ai_message(message: str, response_mime_type: str, system_prompt: Optional[str]) -> str:
    # Gemini AIモデルを生成
    genai.configure(api_key=gemini_api_key)
    # チャットの応答を生成
    model = genai.GenerativeModel(
        "gemini-1.5-flash",
        system_instruction=system_prompt,
    )
    
    # チャットの応答を生成
    response = model.generate_content(
        message,
        generation_config={"response_mime_type": response_mime_type})
    return response.text


'''
問題メッセージを編集する
'''
def edit_question(diary_id: int, question_no: int) -> TextSendMessage :
    # 問題データを取得する
    question = querys.select_question(diary_id, question_no)
    # 選択肢データを取得する
    options = querys.select_option(question.id)
    # 選択肢用のクイックリプライを編集する
    question_and_options = question.question_text
    optionList =[]
    for option in options:
        optionList.append(QuickReplyButton(
            action=MessageAction(
                label=str(option.option_no), 
                text=str(option.option_no))))
        question_and_options += '\n' + str(option.option_no) + '. ' + option.option_text

    # 問題文用のメッセージを返す
    return TextSendMessage(text=question_and_options, quick_reply=QuickReply(items=optionList))
