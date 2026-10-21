"""Transport-only pacing; delegate each request once and disclose waiting."""
import hashlib
import json
from pathlib import Path
import time


class PacedPost:
    def __init__(self,delegate,endpoint,output,interval=60,clock=time.monotonic,sleep=time.sleep):
        self.delegate=delegate;self.endpoint=endpoint;self.output=Path(output)
        self.interval=interval;self.clock=clock;self.sleep=sleep
        self.last_start=clock();self.calls=0

    def __call__(self,url,*args,**kwargs):
        if url!=self.endpoint:return self.delegate(url,*args,**kwargs)
        waited=0.0
        while True:
            remaining=self.last_start+self.interval-self.clock()
            if remaining<=0:break
            step=min(30,remaining);self.sleep(step);waited+=step
        self.last_start=self.clock();self.calls+=1
        response=None;error=None
        try:
            response=self.delegate(url,*args,**kwargs)
            return response
        except Exception as exc:
            error=type(exc).__name__
            raise
        finally:
            record={'sequence':self.calls,'minimum_start_interval_seconds':self.interval,
                'pacing_wait_seconds':waited,'network_seconds':self.clock()-self.last_start,
                'http_status':getattr(response,'status_code',None),'exception_class':error,
                'payload_sha256':hashlib.sha256(json.dumps(kwargs.get('json'),sort_keys=True,ensure_ascii=False).encode()).hexdigest(),
                'limits':'Frozen runner terminal seconds include this pacing wait; do not interpret it as pure provider or architecture latency. No retry is performed by this wrapper.'}
            self.output.mkdir(parents=True,exist_ok=True)
            with (self.output/f'{self.calls:04d}.json').open('x',encoding='utf-8') as f:json.dump(record,f,indent=2)
