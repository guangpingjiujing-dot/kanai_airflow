"""
最もシンプルなAirflow DAG
studentsテーブルにサンプルデータをロードする
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# デフォルト引数
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG定義
dag = DAG(
    'load_students',
    default_args=default_args,
    description='studentsテーブルにサンプルデータをロードする',
    schedule_interval=timedelta(days=1),  # 1日1回実行
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['simple', 'students'],
)


def load_students_data(**context):
    """
    studentsテーブルにデータをロードする関数
    実際の環境では、データベース接続情報を環境変数や設定ファイルから取得してください
    """
    # サンプルデータ
    students_data = [
        (101, '田中 一郎', 'ichiro.tanaka@example.com', '2024-01-05 09:00:00'),
        (102, '高橋 美咲', 'misaki.takahashi@example.com', '2024-01-10 10:30:00'),
        (103, '伊藤 健太', 'kenta.ito@example.com', '2024-01-15 14:00:00'),
        (104, '山本 さくら', 'sakura.yamamoto@example.com', '2024-01-20 11:15:00'),
        (105, '中村 大輔', 'daisuke.nakamura@example.com', '2024-02-01 09:30:00'),
        (106, '小林 麻衣', 'mai.kobayashi@example.com', '2024-02-05 13:45:00'),
    ]
    
    # データベース接続（実際の環境では接続情報を環境変数から取得）
    # 例: connection_string = os.getenv('DB_CONNECTION_STRING')
    # ここでは接続処理をスキップし、ログに出力するだけのシンプルな実装
    
    print(f"ロードするデータ: {len(students_data)}件")
    for student in students_data:
        print(f"  - {student[1]} ({student[2]})")
    
    # 実際のデータベースへの挿入処理
    # 接続情報は環境変数やAirflowのConnectionsから取得してください
    # conn = pyodbc.connect(connection_string)
    # cursor = conn.cursor()
    # for student in students_data:
    #     cursor.execute("""
    #         INSERT INTO [taitechWarehouseTraning].[learning_management_system].[students]
    #         (student_id, name, email, enrollment_date)
    #         VALUES (?, ?, ?, ?)
    #     """, student)
    # conn.commit()
    # conn.close()
    
    print("データロード完了（実際のDB接続はコメントアウトされています）")
    return len(students_data)


# タスク定義
load_task = PythonOperator(
    task_id='load_students',
    python_callable=load_students_data,
    dag=dag,
)

# タスクの実行順序（この場合は1つのタスクのみ）
load_task

