import argparse
import json
import logging

import apache_beam as beam
from apache_beam.io import ReadFromText
# from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions

# set schema
_DAU_TABLE_SCHEMA = {
    'fields': [
        {'name': 'dt', 'type': 'date', 'mode': 'required'},
        {'name': 'paid_users', 'type': 'int64', 'mode': 'required'},
        {'name': 'free_to_play_users', 'type': 'int64', 'mode': 'required'}
    ]
}


class CountUsersFn(beam.CombineFn):
    """課金ユーザと無課金ユーザの人数を集計する。"""
    def create_accumulator(self):
        """課金ユーザと無課金ユーザの人数を保持するaccumulatorを作成して返却する。
        Returns:
        課金ユーザと無課金ユーザの人数を表すタプル(0, 0)
        """
        return 0, 0

    def add_input(self, accumulator, is_paid_user):
        """課金ユーザまたは無課金ユーザの人数を加算する。
        Args:
        accumulator: 課金ユーザと無課金ユーザの人数を表すタプル（現在の中間結果）
        is_paid_user: 課金ユーザであるか否かを表すフラグ
        Returns:
        加算後の課金ユーザと無課金ユーザの人数を表すタプル
        """
        (paid, free) = accumulator
        if is_paid_user:
            return paid + 1, free
        else:
            return paid, free + 1

    def merge_accumulators(self, accumulators):
        """複数のaccumulatorを単一のaccumulatorにマージした結果を返却する。
        Args:
        accumulators: マージ対象の複数のaccumulator
        Returns:
        マージ後のaccumulator
        """
        paid, free = zip(*accumulators)
        return sum(paid), sum(free)

    def extract_output(self, accumulator):
        """集計後の課金ユーザと無課金ユーザの人数を返却する。
        Args:
        accumulator: 課金ユーザと無課金ユーザの人数を表すタプル
        Returns:
        集計後の課金ユーザと無課金ユーザの人数を表すタプル
        """
        return accumulator


def run():
    """This method ,the entrypoint of main process, define the pipeline
    and execute.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--dt',
        dest='dt',
        help='event date'
    )
    known_args, pipeline_args = parser.parse_known_args()
    pipeline_options = PipelineOptions(pipeline_args)

    event_file_path = (
        'gs://gcloud-etl-tutorial/data/events/{}/000000000000.json.gz'.format(
            known_args.dt
            )
        )

    dt = '{}-{}-{}'.format(
        known_args.dt[0:4], known_args.dt[4:6], known_args.dt[6:8]
        )

    with beam.Pipeline(options=pipeline_options) as p:
        user_pseudo_ids = (
            p
            # storage からユーザ行動を読み取る
            | 'Read Events' >> ReadFromText(event_file_path)
            # JSON 形式のデータをパースしてuser_pseudo_idを抽出
            | 'Parse Events' >> beam.Map(
                lambda event: json.loads(event).get('user_pseudo_id')
            )
            # 重複しているuser_pseudo_idを排除
            | 'Deduplicate User Pseudo Ids' >> beam.Distinct()
            # 後続の結合処理で必要となるため、キーバリュー形式にデータを変換する
            | 'Transform to KV' >> beam.Map(
                lambda user_pseudo_id: (user_pseudo_id, None)
            )
        )

        # gcp_tutorialからユーザ情報の一覧を取得する
        users = (
            p
            # load data from gcp_tutorial.users table
            | 'Read Users' >> beam.io.Read(
                beam.io.BigQuerySource('gcp_tutorial.users')
            )
            # 後続の結合処理のためキーバリュー形式にデータを変換する
            | 'Transform Users' >> beam.Map(
                lambda user: (user['user_pseudo_id'], user['is_paid_user'])
            )
        )

        # PCollection user_pseudo_idとusersを結合
        # 集計して、課金ユーザと無課金ユーザそれぞれの人数を算出
        (
            {'user_pseudo_ids': user_pseudo_ids, 'users': users}
            # user_pseudo_idsとusersを結合
            | 'Join' >> beam.CoGroupByKey()
            # ユーザ行動ログが存在するユーザ情報のみ抽出
            | 'Filter Users with Events' >> beam.Filter(
                lambda row: len(row[1]['user_pseudo_ids']) > 0
            )
            | 'Transform to Is Paid User' >> beam.Map(
                lambda row: row[1]['users'][0]
            )
            # 課金ユーザであるか否かを表すフラグを抽出
            | 'Count Users' >> beam.CombineGlobally(CountUsersFn())
            # BigQueryのテーブルへ書き込むためのデータを組み立てる
            | 'Create a Row to BigQuery' >> beam.Map(
                lambda user_nums: {
                    'dt': dt,
                    'paid_users': user_nums[0],
                    'free_to_play_users': user_nums[1]
                }
            )
            # gcp_tutorial.dauへ算出結果を書き込む
            | 'Write a Row to BigQuery' >> beam.io.WriteToBigQuery(
                'gcp_tutorial.dau',
                schema=_DAU_TABLE_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
