import requests
import sys
if sys.platform == 'linux':
    import curses
import pandas as pd
import re
from sqlalchemy import create_engine,text
import os
import json
from concurrent.futures import ThreadPoolExecutor
import time
from functools import reduce,wraps
from threading import Thread,Lock,Event
from typing import Optional
from queue import Queue


class DLMgr:
    def __init__(self,
                 params:list,
                 mgr_num:int=3) -> None:
        """
        params: DLBot的参数
        mgr_num: 下载线程的数量，默认为3
        """
        self.params = params
        self.mgr_num = mgr_num
        self.queue = Queue()
        for p in self.params:
            p['queue'] = self.queue
        self.dl_tasks = dict()
        self.stop_event = Event()
        self.lock = Lock()
        self.dashboard_thread = Thread(target=self.update_dashboard)
        if sys.platform == 'linux':
            self.screen = curses.initscr()

    
    def display_dashboard(self):
        """打印下载面板"""
        if sys.platform == 'linux':
            self.screen.clear()
            self.screen.addstr(0,0,'Downloading Status')        
            self.screen.addstr(1,0,'{:2}| {:50} | {:10} |  {:10} | {:10}'.format('No','file_name','file_size','progress','speed'))
            self.screen.addstr(2,0,'----------------------------------------------------------------------------------------------')
            i=3
            for file_name,data in self.dl_tasks.items():
                sn =i-2
                self.screen.addstr(i,0,f'{sn:2}| {file_name:50} | {data["file_size"]:10} | {data["progress"]:10.2f}% | {data["speed"]:10}')
                i+=1
            self.screen.refresh()
        else:
            os.system('cls')
            print('Downloading Status')
            print('{:2}| {:50} | {:10} |  {:10} | {:10}'.format('No','file_name','file_size','progress','speed'))
            print('----------------------------------------------------------------------------------------------')
            i=3
            for file_name,data in self.dl_tasks.items():
                sn =i-2
                print(f'{sn:2}| {file_name:50} | {data["file_size"]:10} | {data["progress"]:10.2f}% | {data["speed"]:10}')
                i+=1
            
    def update_dashboard(self):
        """更新下载面板数据"""
        while not self.stop_event.is_set():
            try:
                msg= self.queue.get(timeout=3)
            except:
                continue
            if msg.get('status','') == 'complete' and msg['file_name'] in self.dl_tasks:
                self.dl_tasks.pop(msg['file_name'])
                continue
            self.dl_tasks.update(msg)
            self.display_dashboard()

    def start_dl(self):
        self.dashboard_thread.start()
        self._start_dl()
        self.dashboard_thread.join()
    
    def _start_dl(self):
        with ThreadPoolExecutor(max_workers=self.mgr_num) as executer:
            {executer.submit(DLMgr.activate_dlb,self,p): p for p in self.params}
        self.stop_event.set()
        
    def activate_dlb(self,kwargs):
        dlb=DLBot(**kwargs)
        dlb.start_dl()


class DLBot:
    
    def __init__(self, 
                 url:str,
                 folder_path:str ='.',
                 file_name:Optional[str] = None,
                 workers:int = 3,
                 force:bool=False,
                 queue:Optional[Queue] = None
                 ):
        self.url = url
        self.folder_path = re.sub(r':','',folder_path)
        self.file_name = file_name
        self.file_size:Optional[int] = None
        self.file_path:Optional[str] = None 
        self.headers:Optional[dict] = None
        self.tasks:list[dict] = []
        self.status:str = 'na'
        self.block_size:int = 1024*1024*10 # 10mb
        self.chunk_size:int = 1024*64 # 64kb
        self.engine = create_engine('sqlite:///dl_log.db')
        self.workers = workers
        self.force = force
        self.queue = queue
        self.speed_thread = Thread(target=self.speed_review)
        self.db_not_init = False


    def log_to_db(self):
        """
        在数据库中记录日志
        """
        data = [[self.url,self.file_name,self.file_path,self.file_size,json.dumps(self.headers),json.dumps(self.tasks),self.workers,self.status]]
        df = pd.DataFrame(data,columns=['url','file_name','file_path','file_size','headers','tasks','workers','status'])
        df.set_index('url',inplace=True,drop=True) 
        with self.engine.connect() as con:
            try:
                con.execute(text(f'''delete from dl_log where url='{self.url}' ;'''))
                df.to_sql(name='dl_log',con=con,if_exists='append',index=True)
            except:
                df.to_sql(name='dl_log',con=con,if_exists='replace',index=True)
            con.commit()
            
    def _load_from_db(self,sql):
        """
        读取数据库中信息
        """
        with self.engine.connect() as con:
            return pd.read_sql(text(sql),con=con) 
        
    def load_all_from_db(self):
        """
        返回所有记录
        """
        sql = f'''select * from dl_log;'''
        df = self._load_from_db(sql)
        return df
        
    def is_complete(self)->bool:
        """
        判断文件是否下载完成
        """
        try:
            sql = f'''select * from dl_log where url='{self.url}';'''
            df = self._load_from_db(sql)
            if df.empty :
                return False
            elif os.path.exists(df['file_path'].values[0]) is False:
                return False
            elif df['file_size'].values[0] == os.path.getsize(df['file_path'].values[0]):
                return True
            
            self.workers = int(df['workers'].values[0])
            self.tasks = json.loads(df['tasks'].values[0])
            return False
        except:
            return False
        
    def is_tasks_in_db(self)->bool:
        """
        判断任务是否存在本地数据库
        """
        try:
            sql = f'''select * from dl_log where url='{self.url}';'''
            df = self._load_from_db(sql)
            if df.empty or json.loads(df['tasks'].values[0]) == []:
                return False
            else:
                self.tasks = json.loads(df['tasks'].values[0])
                return True    
        except:
            False
        
    def fetch_headers(self):
        """
        获取html headers
        """
        r = requests.get(self.url, stream=True)
        self.headers = dict(r.headers)
        self.file_size = int(r.headers['Content-Length'])
        file_name = r.headers.get('X-File-Name',self.url.split('/')[-1])
        self.file_name = re.sub(r'[/\?:"<>|\*\n\t]', '', file_name) if self.file_name is None else self.file_name
        self.file_path = os.path.join(self.folder_path,self.file_name)
        if (os.path.exists(self.file_path)) and (os.path.getsize(self.file_path) == self.file_size):      
            resp = input('文件已存在，且内容完整，是否覆盖(y/n)?') 
            if resp.lower() == 'n':
                self.status = 'complete'

    def split_task(self):
        """
        分拆线程任务
        """
        for i in range(self.file_size//self.block_size+1):
            bid = i
            st = i*self.block_size+1 if i>0 else 0
            ed = min(self.file_size,(i+1)*self.block_size)
            bs = ed-st
            blk_path = os.path.join(self.folder_path,f'{self.file_name}_{bid}')
            loaded = 0
            status = 'na'
            self.tasks.append(dict(bid=bid,st=st,ed=ed,bs=bs,blk_path=blk_path,loaded=loaded,status=status))

    @property
    def file_size_str(self):
        return f'{self.file_size/1024/1024:.2f}MB'
    
    @property
    def speed_str(self):
        return f'{self.speed/1024/1024:.2f}MB/s'

    def speed_review(self):
        """
        统计下行速率
        """
        last_review_time = None
        last_review_sz = None 
        
        while self.status!='complete':
            if last_review_time is None:
                time.sleep(1)
                last_review_time = time.time()
                if self.tasks == []:
                    time.sleep(1)
                    continue
                last_review_sz = reduce(lambda x,y:x+y,list(map(lambda x:x.get('loaded',0),self.tasks)))
                if last_review_sz == self.file_size:
                    self.queue.put({self.file_name:dict(file_size=self.file_size_str,speed=0,progress=100)})
                    print('下载完成')
                    self.status = 'complete'
                    break
                continue
            
            now_review_time = time.time()
            now_review_sz = reduce(lambda x,y:x+y,list(map(lambda x:x.get('loaded',0),self.tasks))) 
            
            self.speed = (now_review_sz-last_review_sz)/(now_review_time-last_review_time)
            progress = now_review_sz / self.file_size * 100
             
            if self.queue is not None:
                self.queue.put({self.file_name:dict(file_size=self.file_size_str,speed=self.speed_str,progress=progress)})
            
            last_review_time = now_review_time
            last_review_sz = now_review_sz
            
            time.sleep(5)

        
    def prepare_dl(self)->bool:
        # 检查路径
        if not os.path.exists(self.folder_path):
            os.makedirs(self.folder_path)
        # 查看数据库中记录是否显示已经完成
        if not self.force and self.is_complete():
            resp = input(f'数据库中标记已完成，是否继续(y/n)')
            if resp.lower() == 'n':
                return 
        # 获得文件数据
        self.fetch_headers()
        
        # 分配任务
        if self.is_tasks_in_db() is False:
            self.split_task()
            
        # 记录状态
        self.log_to_db()
        
        if self.status == 'complete':
            print(self.url,'下载完成',end='\r')
            return False
        
        return True
    
    def start_speed_review(func):
        @wraps(func)
        def inner(self,*args,**kwargs):
            self.speed_thread.start()
            func(self,*args,**kwargs)
            self.speed_thread.join()
        return inner
        
        
    def process_dl(self):
        # self.speed_thread.start()
        with ThreadPoolExecutor(max_workers=self.workers) as executer:
            {executer.submit(DLBot.activate,self,**p): p for p in self.tasks}
 
            
    def merge_dl(self):
                
        with open(self.file_path,'wb') as f:
            for tasks in self.tasks:
                with open(tasks['blk_path'],'rb') as bf:
                    f.write(bf.read())
                    
        self.status = 'complete'
        # self.speed_thread.join()
        self.log_to_db()

        for tasks in self.tasks:
            os.remove(tasks['blk_path'])
    
    
    def activate(self,
                 bid:int,
                 st:int,
                 ed:int,
                 bs:int,
                 blk_path:str,
                 **kwargs):
        
        local_sz = os.path.getsize(blk_path) if os.path.exists(blk_path) else 0
        
        # 如果本地文件和任务文件大小一致，视为完成任务
        if local_sz == bs:
            self.tasks[bid]['loaded']=local_sz
            self.tasks[bid]['status']='complete'
            return 
        
        assert local_sz <= bs,'本地文件数据大于任务数据！！'
        
        loading = local_sz + st
        
        headers={
            'Range':f'bytes={loading}-{ed}',
            'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36'
        }
                
        try:
            resp = requests.get(self.url,stream=True,headers=headers)

            with open(blk_path,'ab') as f:
                f.seek(loading-st)
                for chunk in resp.iter_content(chunk_size=self.chunk_size):
                    if chunk:
                        local_sz += len(chunk)
                        f.write(chunk)
                        f.flush()        
                        self.tasks[bid]['loaded']=local_sz

            self.tasks[bid]['status']='complete'
            
        except Exception as e:
            print(blk_path,e,'一分钟后重新下载',end='\r')
            self.tasks[bid]['status']=e
            self.log_to_db()
            time.sleep(60)
            self.activate(bid,st,ed,bs,blk_path)
    
    def start_dl(self):
        if self.prepare_dl():
            self.speed_thread.start()
            self.process_dl()
            self.merge_dl()
            self.speed_thread.join()


if __name__=='__main__':
    url = input('输入下载链接：')
    dl = DLMgr(params=[{'url':url}])
    dl.start_dl()
