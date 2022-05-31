import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit, to_date, when


def run():
    """entrypoint for main process"""
    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket', dest='bucket', help='Cloud Storage bucket')
    parser.add_argument('--dt', dest='dt', help='event date')
    args = parser.parse_args()

    # create spark settion
    spark = SparkSession.builder.appName('CountUsers').getOrCreate()
    # BigQuery のテーブルへのロード時に使用されるCloud Storageのバケットを設定
    spark.conf.set('temporaryGcsBucket', args.bucket)

    # ファイル読み取り対象のCloud Storage のパスを組み立てる
    event_file_path = 'gs://{}/data/events/{}/*.json.gz'\
        .format(args.bucket, args.dt)
    # 処理対象のイベント日付を"YYYY-MM-DD"形式で組み立てる。
    dt = '{}-{}-{}'.format(args.dt[0:4], args.dt[4:6], args.dt[6:8])

    # Cloud Storageからユーザ行動ログを読み取りuser_pseudo_idの一覧を抽出
    user_pseudo_ids = spark.read.json(
        event_file_path
        ).select('user_pseudo_id').distinct()

    # gcp_tutorialからデータを読み取る
    users = spark.read.format('bigquery').load('gcp_tutorial.users')

    user_pseudo_ids.join(users, 'user_pseudo_id')\
        .agg(count(when(col('is_paid_user'), 1)).alias('paid_users'),
             count(when(~col('is_paid_user'), 1)).alias('free_to_play_users'))\
        .withColumn('dt', to_date(lit(dt)))\
        .select('dt', 'paid_users', 'free_to_play_users')\
        .write.format('bigquery').option('table', 'gcp_tutorial.dau')\
        .mode('append').save()

    # SparkSessionに紐づくSparkContextを停止
    spark.stop()


if __name__ == '__main__':
    run()
