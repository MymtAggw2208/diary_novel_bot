from dataclasses import dataclass
from datetime import date
from typing import Optional

@dataclass
class UserStatus:
    '''ユーザーステータステーブルのデータクラス'''
    user_id: str
    status: Optional[str]
    current_diary_id: Optional[int]
    current_question_no: Optional[int]
    latest_diary_date: date


@dataclass
class Diary:
    '''日記テーブルのデータクラス'''
    id: int
    user_id: str
    diary_date: date
    original_text: Optional[str]
    english_text: Optional[str]
    japanese_text: Optional[str]
    number_of_correct_answers: int

@dataclass
class Question:
    '''問題テーブルのデータクラス'''
    id: int
    diary_id: int
    question_no: int
    question_text: str
    explanation_text: str
    mistake_flag: Optional[bool]

@dataclass
class Options:
    '''選択肢テーブルのデータクラス'''
    id: int
    question_id: int
    option_no: int
    option_text: str
    correct_flag: bool