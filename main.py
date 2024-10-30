from flask import abort, jsonify
import os
import base64, hashlib, hmac
import google.generativeai as genai
import json

from linebot import (
    LineBotApi, WebhookParser
)
from linebot.exceptions import (
    InvalidSignatureError
)
from linebot.models import (
    MessageEvent, TextMessage, TextSendMessage
)

# CHANNEL_ACCESS_TOKEN
channel_access_token = os.environ.get('LINE_CHANNEL_ACCESS_TOKEN')
# CANNEL_SECRET
channel_secret = os.environ.get('LINE_CHANNEL_SECRET')
# GEMINI_APIKEY
gemini_api_key = os.environ.get('GEMINI_API_KEY')
# 日記作成のシステムプロンプト
system_prompt_diary = f"""
与えられた文章から日記を生成し、「original」「translation」の2項目で構成されるJSON形式で答えてください。
その際、以下の条件に沿って回答してください。
1. 与えられた文章に場面描写や心情、セリフを追加して、最大500文字以内の短い物語にしてください。
2. 次に、物語を英文にしてください。
3. 英訳の際、文中のセリフはアガサ・クリスティーの「名探偵ポワロ」の登場人物のような言い回しにしてください。
   例：'My name is Hercule Poirot, and I am probably the greatest detective in the world.'
       'It is the brain, the little gray cells on which one must rely. One must seek the truth within - not without.'
       'Oui, mademoiselle. But Monsieur Martin was not alone in his dislike for Lady Edgware. You, Miss Carroll. When I asked you the identity of the woman you saw entering the hall of Regent Gate...'
       'C'est ça. Why did you enter the compartment of Madame Kettering?'
       'Three weeks is an eternity to a brain like mine. Without the constant stimulation, my little grey cells will starve and die.'
       'It is a profound belief of mine that if you can induce a person to talk to you for long enough, on any subject whatever! Sooner or later they will give themselves away.'
       'It is an art, the growing of the moustache! I have sympathy for all who attempt it.'
       'Oh, mon pauvre Hastings. But you must not brood! You must occupy yourself, eh?'
       'Hastings, a little something for you, my friend?'
       'The tallest books go in the top shelf, the next tallest in the row beneath, and so on. Thus we have order, method.'
4. 次に日本語訳をつけてください。
5. JSON上、英文は「original」に設定してください。日本語訳は「translation」に設定してください。
"""

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
        if isinstance(event, MessageEvent):
            # メッセージを受信した場合
            if isinstance(event.message, TextMessage):
                # テキストメッセージなら返信作成
                reply_data = []
                # Gemini AIモデルを生成
                genai.configure(api_key=gemini_api_key)
                # チャットの応答を生成
                model_for_diary = genai.GenerativeModel(
                    "gemini-1.5-flash",
                    system_instruction=system_prompt_diary,
                )
                response = model_for_diary.generate_content(
                    event.message.text,
                    generation_config={"response_mime_type": "application/json"})
                data = json.loads(response.text)
                reply_data.append(TextSendMessage(text=data['original']))
                reply_data.append(TextSendMessage(text=data['translation']))

                # 応答内容をLINEで送信
                line_bot_api.reply_message(
                        event.reply_token,
                        reply_data
                    ) 
            else:
                continue

    return jsonify({ 'message': 'ok'})
