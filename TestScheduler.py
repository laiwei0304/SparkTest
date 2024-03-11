from apscheduler.schedulers.blocking import BlockingScheduler
from models.match.DoGenderTag import DoGenderTag


def do_gender_tag():
    DoGenderTag.start()


sche = BlockingScheduler()

# 在每年3月11日16:39执行do_gender_tag任务
sche.add_job(do_gender_tag, 'cron', month='3', day='11', hour='16', minute='39')

sche.start()
