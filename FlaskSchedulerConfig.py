from models.match.DoGenderTag import DoGenderTag
from models.match.DoJobTag import DoJobTag
from models.match.DoPoliticalFaceTag import DoPoliticalFaceTag
from models.match.DoMarriageTag import DoMarriageTag
from models.match.DoNationalityTag import DoNationalityTag
from models.match.DoIsBlackListTag import DoIsBlackListTag
from models.mining.DoUsgTag import DoUsgTag
from models.mining.DoRfmTag import DoRfmTag
from models.mining.DoPsmTag import DoPsmTag
from models.mining.DoRfeTag import DoRfeTag
from flask_apscheduler.auth import HTTPBasicAuth
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore


class Config(object):
    JOBS = [
        # interval定时执行（从start_date到end_date，间隔20s，包含首尾）
        # func也可以写字符串形式，例如：'App.tasks.DatabaseTask:send_ding_test'
        # {
        #     'id': '',
        #     'func': DoGenderTag.start,
        #     'trigger': 'interval',
        #     'start_date': '2021-01-27 13:31:00',
        #     'end_date': '2021-01-27 13:33:00',
        #     'seconds': 20,
        #     'replace_existing': True  # 重新执行程序时，会将jobStore中的任务替换掉
        # },
        # date一次执行
        # {
        #     'id': '',
        #     'func': DoGenderTag.start,
        #     'trigger': 'date',
        #     'run_date': '2024-03-12 11:00:00',
        #     'replace_existing': True
        # },
        # cron式定时调度，类似linux的crontab
        {
            'id': 'do_gender_tag',
            'func': DoGenderTag.start,
            'trigger': 'cron',
            'month': '3',
            'day': '12',
            'hour': '11',
            'minute': '5',
            'second': '0',
            'replace_existing': True
        },
        {
            'id': 'do_job_tag',
            'func': DoJobTag.start,
            'trigger': 'cron',
            'month': '3',
            'day': '12',
            'hour': '11',
            'minute': '5',
            'second': '0',
            'replace_existing': True
        },
        {
            'id': 'do_isBlackList_tag',
            'func': DoIsBlackListTag.start,
            'trigger': 'cron',
            'month': '3',
            'day': '12',
            'hour': '11',
            'minute': '5',
            'second': '0',
            'replace_existing': True
        },
        {
            'id': 'do_marriage_tag',
            'func': DoMarriageTag.start,
            'trigger': 'cron',
            'month': '3',
            'day': '12',
            'hour': '11',
            'minute': '5',
            'second': '0',
            'replace_existing': True
        },
        {
            'id': 'do_nationality_tag',
            'func': DoNationalityTag.start,
            'trigger': 'cron',
            'month': '3',
            'day': '12',
            'hour': '11',
            'minute': '5',
            'second': '0',
            'replace_existing': True
        },
        {
            'id': 'do_politicalFace_tag',
            'func': DoPoliticalFaceTag.start,
            'trigger': 'cron',
            'month': '3',
            'day': '12',
            'hour': '11',
            'minute': '5',
            'second': '0',
            'replace_existing': True
        },
        {
            'id': 'do_psm_tag',
            'func': DoPsmTag.start,
            'trigger': 'cron',
            'month': '3',
            'day': '12',
            'hour': '11',
            'minute': '5',
            'second': '0',
            'replace_existing': True
        },
        {
            'id': 'do_rfe_tag',
            'func': DoRfeTag.start,
            'trigger': 'cron',
            'month': '3',
            'day': '12',
            'hour': '11',
            'minute': '5',
            'second': '0',
            'replace_existing': True
        },
        {
            'id': 'do_rfm_tag',
            'func': DoRfmTag.start,
            'trigger': 'cron',
            'month': '3',
            'day': '12',
            'hour': '11',
            'minute': '5',
            'second': '0',
            'replace_existing': True
        },
        {
            'id': 'do_usg_tag',
            'func': DoUsgTag.start,
            'trigger': 'cron',
            'month': '3',
            'day': '12',
            'hour': '11',
            'minute': '5',
            'second': '0',
            'replace_existing': True
        },
        # {
        #     'id': 'do_job_tag',
        #     'func': DoJobTag.start,
        #     'trigger': 'cron',
        #     'month': '3',
        #     'day': '12',
        #     'hour': '11',
        #     'minute': '5',
        #     'second': '0',
        #     'replace_existing': True
        # },
        # {
        #     'id': 'do_job_tag',
        #     'func': DoJobTag.start,
        #     'trigger': 'cron',
        #     'month': '3',
        #     'day': '12',
        #     'hour': '11',
        #     'minute': '5',
        #     'second': '0',
        #     'replace_existing': True
        # },
        # {
        #     'id': 'do_job_tag',
        #     'func': DoJobTag.start,
        #     'trigger': 'cron',
        #     'month': '3',
        #     'day': '12',
        #     'hour': '11',
        #     'minute': '5',
        #     'second': '0',
        #     'replace_existing': True
        # },
        # {
        #     'id': 'do_job_tag',
        #     'func': DoJobTag.start,
        #     'trigger': 'cron',
        #     'month': '3',
        #     'day': '12',
        #     'hour': '11',
        #     'minute': '5',
        #     'second': '0',
        #     'replace_existing': True
        # },
        # {
        #     'id': 'do_job_tag',
        #     'func': DoJobTag.start,
        #     'trigger': 'cron',
        #     'month': '3',
        #     'day': '12',
        #     'hour': '11',
        #     'minute': '5',
        #     'second': '0',
        #     'replace_existing': True
        # },
        # {
        #     'id': 'do_job_tag',
        #     'func': DoJobTag.start,
        #     'trigger': 'cron',
        #     'month': '3',
        #     'day': '12',
        #     'hour': '11',
        #     'minute': '5',
        #     'second': '0',
        #     'replace_existing': True
        # },
        # {
        #     'id': 'do_job_tag',
        #     'func': DoJobTag.start,
        #     'trigger': 'cron',
        #     'month': '3',
        #     'day': '12',
        #     'hour': '11',
        #     'minute': '5',
        #     'second': '0',
        #     'replace_existing': True
        # },
        # {
        #     'id': 'do_job_tag',
        #     'func': DoJobTag.start,
        #     'trigger': 'cron',
        #     'month': '3',
        #     'day': '12',
        #     'hour': '11',
        #     'minute': '5',
        #     'second': '0',
        #     'replace_existing': True
        # },
        # {
        #     'id': 'do_job_tag',
        #     'func': DoJobTag.start,
        #     'trigger': 'cron',
        #     'month': '3',
        #     'day': '12',
        #     'hour': '11',
        #     'minute': '5',
        #     'second': '0',
        #     'replace_existing': True
        # },
        # {
        #     'id': 'do_job_tag',
        #     'func': DoJobTag.start,
        #     'trigger': 'cron',
        #     'month': '3',
        #     'day': '12',
        #     'hour': '11',
        #     'minute': '5',
        #     'second': '0',
        #     'replace_existing': True
        # },
        # {
        #     'id': 'do_job_tag',
        #     'func': DoJobTag.start,
        #     'trigger': 'cron',
        #     'month': '3',
        #     'day': '12',
        #     'hour': '11',
        #     'minute': '5',
        #     'second': '0',
        #     'replace_existing': True
        # },
    ]

    # 存储定时任务（默认是存储在内存中）
    # SCHEDULER_JOBSTORES = {'default': SQLAlchemyJobStore(url='mysql+pymysql://xxx/xx')}
    # 设置时区，时区不一致会导致定时任务的时间错误
    SCHEDULER_TIMEZONE = 'Asia/Shanghai'
    # 一定要开启API功能，这样才可以用api的方式去查看和修改定时任务
    SCHEDULER_API_ENABLED = True
    # api前缀（默认是/scheduler）
    SCHEDULER_API_PREFIX = '/scheduler'
    # 配置允许执行定时任务的主机名
    SCHEDULER_ALLOWED_HOSTS = ['*']
    # auth验证。默认是关闭的，
    # SCHEDULER_AUTH = HTTPBasicAuth()
    # 设置定时任务的执行器（默认是最大执行数量为10的线程池）
    SCHEDULER_EXECUTORS = {'default': {'type': 'threadpool', 'max_workers': 30}}
    # 另外flask-apscheduler内有日志记录器。name为apscheduler.scheduler和apscheduler.executors.default。如果需要保存日志，则需要对此日志记录器进行配置
