from google.cloud import bigquery
import os
from logging import getLogger
from models import UserStatus, Diary, Question, Options
from typing import Optional, List

'''
環境変数
'''
# ユーザーステータステーブルID
table_id_user_status = os.environ.get('TABLE_ID_USER_STATUS')
# 日記テーブルID
table_id_diary = os.environ.get('TABLE_ID_DIARY')
# 問題テーブルID
table_id_question = os.environ.get('TABLE_ID_QUESTION')
# 選択肢テーブルID
table_id_options = os.environ.get('TABLE_ID_OPTIONS')

# BigQueryインスタンスの作成
client = bigquery.Client()
# loggerの取得
logger = getLogger(__name__)


'''
ユーザーステータステーブルINSERT
'''
def insert_user_status(userStatus: UserStatus):
    # クエリを生成
    query = f'''INSERT INTO `{table_id_user_status}`
                (user_id, status, current_diary_id, current_question_no, latest_diary_date)
                VALUES
                (@user_id, @status, @current_diary_id, @current_question_no, @latest_diary_date)
            '''
    # データをパラメータに変換
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter('user_id', 'STRING', userStatus.user_id),
            bigquery.ScalarQueryParameter('status', 'STRING', userStatus.status),
            bigquery.ScalarQueryParameter('current_diary_id', 'INTEGER', userStatus.current_diary_id),
            bigquery.ScalarQueryParameter('current_question_no', 'INTEGER', userStatus.current_question_no),
            bigquery.ScalarQueryParameter('latest_diary_date', 'DATE', userStatus.latest_diary_date)
        ]
    )

    try:
        # クエリの実行
        query_job = client.query(query, job_config=job_config)
        query_job.result()
    except Exception as e:
        # 例外が発生した場合、ログにエラーを出力
        logger.error(f'insert_user_statusでエラー発生：{e}')


'''
日記テーブルINSERT
'''
def insert_diary(diaryEntry: Diary) -> Optional[int]:
    # ID採番
    id = get_id(table_id_diary)
    # クエリを生成
    query = f'''INSERT INTO `{table_id_diary}`
                (id, user_id, diary_date, original_text, english_text, 
                 japanese_text, number_of_correct_answers)
                VALUES
                (@id, @user_id, @diary_date, @original_text, @english_text,
                 @japanese_text, @number_of_correct_answers)
            '''
    # データをパラメータに変換
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter('id', 'INTEGER', id),
            bigquery.ScalarQueryParameter('user_id', 'STRING', diaryEntry.user_id),
            bigquery.ScalarQueryParameter('diary_date', 'DATE', diaryEntry.diary_date),
            bigquery.ScalarQueryParameter('original_text', 'STRING', diaryEntry.original_text),
            bigquery.ScalarQueryParameter('english_text', 'STRING', diaryEntry.english_text),
            bigquery.ScalarQueryParameter('japanese_text', 'STRING', diaryEntry.japanese_text),
            bigquery.ScalarQueryParameter('number_of_correct_answers', 'INTEGER', diaryEntry.number_of_correct_answers)
        ]
    )

    try:
        # クエリの実行
        query_job = client.query(query, job_config=job_config)
        query_job.result()
        return id
    except Exception as e:
        # 例外が発生した場合、ログにエラーを出力
        logger.error(f'insert_diaryでエラー発生：{e}')
        return None


'''
問題テーブルINSERT
'''
def insert_question(questionEntry: Question) -> Optional[int]:
    # ID採番
    id = get_id(table_id_question)
    # クエリを生成
    query = f'''INSERT INTO `{table_id_question}`
                (id, diary_id, question_no, question_text, explanation_text)
                VALUES
                (@id, @diary_id, @question_no, @question_text, @explanation_text)
            '''
    # データをパラメータに変換
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter('id', 'INTEGER', id),
            bigquery.ScalarQueryParameter('diary_id', 'INTEGER', questionEntry.diary_id),
            bigquery.ScalarQueryParameter('question_no', 'INTEGER', questionEntry.question_no),
            bigquery.ScalarQueryParameter('question_text', 'STRING', questionEntry.question_text),
            bigquery.ScalarQueryParameter('explanation_text', 'STRING', questionEntry.explanation_text)
        ]
    )

    try:
        # クエリの実行
        query_job = client.query(query, job_config=job_config)
        query_job.result()
        return id
    except Exception as e:
        # 例外が発生した場合、ログにエラーを出力
        logger.error(f'insert_questionでエラー発生：{e}')
        return None


'''
選択肢テーブルINSERT
'''
def insert_option(optionEntry: Options):
    # ID採番
    id = get_id(table_id_options)
    # クエリを生成
    query = f'''INSERT INTO `{table_id_options}`
                (id, question_id, option_no, option_text, correct_flag)
                VALUES
                (@id, @question_id, @option_no, @option_text, @correct_flag)
            '''
    # データをパラメータに変換
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                'id', 'INTEGER', id),
            bigquery.ScalarQueryParameter(
                'question_id', 'INTEGER', optionEntry.question_id),
            bigquery.ScalarQueryParameter(
                'option_no', 'INTEGER', optionEntry.option_no),
            bigquery.ScalarQueryParameter(
                'option_text', 'STRING', optionEntry.option_text),
            bigquery.ScalarQueryParameter(
                'correct_flag', 'BOOL', optionEntry.correct_flag)
        ]
    )

    try:
        # クエリの実行
        query_job = client.query(query, job_config=job_config)
        query_job.result()
    except Exception as e:
        # 例外が発生した場合、ログにエラーを出力
        logger.error(f'insert_optionでエラー発生：{e}')


'''
ユーザーステータステーブルSELECT（ユーザーIDから取得）
'''
def select_user_status(user_id: str) -> Optional[UserStatus]:
    # クエリを生成
    query = f'''SELECT * FROM `{table_id_user_status}`
                WHERE user_id = @user_id
            '''
    # データをパラメータに変換
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                'user_id', 'STRING', user_id)
        ]
    )

    try:
        # クエリの実行
        query_job = client.query(query, job_config=job_config)
        result = list(query_job.result())
        if not result:
            # データを取得できなかった場合はNoneを返す
            return None
        # 1件目のデータを返す
        row = result[0]
        return UserStatus(
            user_id=row.user_id,
            status=row.status,
            current_diary_id=row.current_diary_id,
            current_question_no=row.current_question_no,
            latest_diary_date=row.latest_diary_date
        )
    except Exception as e:
        # 例外が発生した場合、ログにエラーを出力
        logger.error(f'select_user_statusでエラー発生：{e}')
        return None


'''
日記テーブルSELECT（IDから取得）
'''
def select_diary(id: int) -> Optional[Diary]:
    # クエリを生成
    query = f'''SELECT * FROM `{table_id_diary}` WHERE id = @id
            '''
    # データをパラメータに変換
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                'id', 'INTEGER', id)
        ]
    )

    try:
        # クエリの実行
        query_job = client.query(query, job_config=job_config)
        result = list(query_job.result())
        if not result:
            # データを取得できなかった場合はNoneを返す
            return None
        # 取得したデータの1件目を返す
        row = result[0]
        return Diary(
            id=row.id,
            user_id=row.user_id,
            diary_date=row.diary_date,
            original_text=row.original_text,
            english_text=row.english_text,
            japanese_text=row.japanese_text,
            number_of_correct_answers=row.number_of_correct_answers
        )
    except Exception as e:
        # 例外が発生した場合、ログにエラーを出力
        logger.error(f'select_diaryでエラー発生：{e}')
        return None


'''
質問テーブルSELECT（日記IDと問題番号から取得）
'''
def select_question(diary_id: int, question_no: int) -> Optional[Question]:
    # クエリを生成
    query = f'''SELECT * FROM `{table_id_question}`
                WHERE diary_id = @diary_id AND question_no = @question_no
            '''
    # データをパラメータに変換
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                'diary_id', 'INTEGER', diary_id),
            bigquery.ScalarQueryParameter(
                'question_no', 'INTEGER', question_no)
        ]
    )
    
    try:
        # クエリ実行
        query_job = client.query(query, job_config=job_config)
        result = list(query_job.result())
        if not result:
            # データを取得できなかった場合はNoneを返す
            return None
        # 取得したデータの1件目を返す
        row = result[0]
        return Question(
            id=row.id,
            diary_id=row.diary_id,
            question_no=row.question_no,
            question_text=row.question_text,
            explanation_text=row.explanation_text,
            mistake_flag=row.mistake_flag
        )
    except Exception as e:
        # 例外が発生した場合、ログにエラーを出力
        logger.error(f'select_questionでエラー発生：{e}')
        return None


'''
選択肢テーブルSELECT（問題IDから取得）
'''
def select_option(question_id: int) -> List[Options]:
    # クエリを生成
    query = f'''SELECT * FROM `{table_id_options}`
                WHERE question_id = @question_id
                ORDER BY option_no
            '''
    # データをパラメータに変換
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                'question_id', 'INTEGER', question_id)
        ]
    )

    try:
        # クエリ実行
        query_job = client.query(query, job_config=job_config)
        result = list(query_job.result())
        if not result:
            # データを取得できなかった場合は空のリストを返す
            return []
        # 取得したデータをリストに変換
        rows = []
        for row in result:
            rows.append(Options(
                id=row.id,
                question_id=row.question_id,
                option_no=row.option_no,
                option_text=row.option_text,
                correct_flag=row.correct_flag))
        return rows
    except Exception as e:
        # 例外が発生した場合、ログにエラーを出力
        logger.error(f'select_optionでエラー発生：{e}')
        return []


'''
正解判定（選択肢テーブルを問題番号と選択肢番号、正解フラグで取得してデータがあればtrueを返す）
'''
def is_correct(question_id: int,option_no: int) -> bool:
    # クエリを生成
    query = f'''SELECT correct_flag FROM `{table_id_options}`
                WHERE question_id = @question_id AND option_no = @option_no AND correct_flag = true
            '''
    # データをパラメータに変換
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                'question_id', 'INTEGER', question_id),
            bigquery.ScalarQueryParameter(
                'option_no', 'INTEGER', option_no)
        ]
    )

    try:
        # クエリ実行
        query_job = client.query(query, job_config=job_config)
        result = list(query_job.result())
        if not result:
            # データを取得できなかった場合はFalseを返す
            return False
        # データを取得している場合はTrueを返す
        row = result[0]
        return row.correct_flag
    except Exception as e:
        # 例外が発生した場合、ログにエラーを出力
        logger.error(f'is_correctでエラー発生：{e}')
        return False


'''
ユーザーステータステーブル更新（ステータス、処理中の日記ID、処理中の問題ID、最新の日記日付を更新する）
'''
def update_user_status(userStatus: UserStatus):
    # クエリを生成
    query = f'''UPDATE `{table_id_user_status}`
                SET status = @status, current_diary_id = @current_diary_id, current_question_no = @current_question_no, latest_diary_date = @latest_diary_date
                WHERE user_id = @user_id
            '''
    # データをパラメータに変換
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                'user_id', 'STRING', userStatus.user_id),
            bigquery.ScalarQueryParameter(
                'status', 'STRING', userStatus.status),
            bigquery.ScalarQueryParameter(
                'current_diary_id', 'INTEGER', userStatus.current_diary_id),
            bigquery.ScalarQueryParameter(
                'current_question_no', 'INTEGER', userStatus.current_question_no),
            bigquery.ScalarQueryParameter(
                'latest_diary_date', 'DATE', userStatus.latest_diary_date)
        ]
    )

    try:
        # クエリ実行
        query_job = client.query(query, job_config=job_config)
        query_job.result()
    except Exception as e:
        # 例外が発生した場合、ログにエラーを出力
        logger.error(f'update_user_statusでエラー発生：{e}')



'''
日記テーブル更新（正解数を更新する）
'''
def update_diary(diaryEntry: Diary):
    # クエリを生成
    query = f'''UPDATE `{table_id_diary}`
                SET number_of_correct_answers = @number_of_correct_answers 
                WHERE id = @id
            '''
    # データをパラメータに変換
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                'id', 'INTEGER', diaryEntry.id),
            bigquery.ScalarQueryParameter(
                'number_of_correct_answers', 'INTEGER', diaryEntry.number_of_correct_answers)
        ]
    )

    try:
        # クエリ実行
        query_job = client.query(query, job_config=job_config)
        query_job.result()
    except Exception as e:
        # 例外が発生した場合、ログにエラーを出力
        logger.error(f'update_diaryでエラー発生：{e}')


'''
問題テーブル更新（誤答フラグを更新する）
'''
def update_question(question_id: int):
    # クエリを生成
    query = f'''UPDATE `{table_id_question}`
                SET mistake_flag = true
                WHERE id = @id
            '''
    # データをパラメータに変換
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                'id', 'INTEGER', question_id)
        ]
    )

    try:
        # クエリ実行
        query_job = client.query(query, job_config=job_config)
        query_job.result()
    except Exception as e:
        # 例外が発生した場合、ログにエラーを出力
        logger.error(f'update_questionでエラー発生：{e}')


'''
ID採番
    指定したテーブル上の最大IDを取得して+1した値を返す
    テーブル上にデータが無い場合は1を返す
'''
def get_id(table_id) -> Optional[int]:
    # テーブルの最大IDを取得
    query = f'''SELECT COALESCE(MAX(id), 0) + 1 as next_id 
            FROM `{table_id}`
            ''' 
    query_job = client.query(query)
    result = list(query_job.result())

    if not result:
        # 結果が取得できなかった場合は1を返す
        return 1
    
    return result[0].next_id
