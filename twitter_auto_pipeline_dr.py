'''
2022-3-1, Zung-Ru
'''
import os
from os import listdir
from datetime import datetime,timedelta
import signal
import subprocess
import time



def auto_pipeline(keyword, sleep_time, folder_path, running_path):
    auto = twarc_pipeline(keyword = keyword, sleep_time = sleep_time, folder_path = folder_path, running_path=running_path)
    auto.filtering_run()




class twarc_pipeline:

    def __init__(self, keyword, sleep_time, folder_path, running_path):
        self.keyword = keyword
        self.sleep_time = sleep_time
        self.folder_path = folder_path
        self.ind = True     
        self.running_path = running_path




    def filtering_run(self):

        abc = ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z',
        'za','zb','zc','zd','ze','zf','zg','zh','zi','zj','zk','zl','zm','zn','zo','zp','zq','zr','zs','zt','zu','zv','zw','zx','zy','zz',
        'zza','zzb','zzc','zzd','zze','zzf','zzg','zzh','zzi','zzj','zzk','zzl','zzm','zzn','zzo','zzp','zzq','zzr','zzs','zzt','zzu','zzv','zzw','zzx','zzy','zzz',
        'zzza','zzzb','zzzc','zzzd','zzze','zzzf','zzzg','zzzh','zzzi','zzzj','zzzk','zzzl','zzzm','zzzn','zzzo','zzzp','zzzq','zzzr','zzzs','zzzt','zzzu','zzzv','zzzw','zzzx','zzzy','zzzz']
        abc_num = 0
        prev_string = datetime.today().strftime("%m%d%Y")



        while self.ind:

            folder_files = [f for f in listdir(self.folder_path)] # all files we have in the folder
            running_files = [f for f in listdir(self.running_path)] # all files we have in the folder
            

            date_string =  datetime.today().strftime("%m%d%Y")

            # check if date changes >> reset a, b, c
            abc_num == 0
            

            new_file = f'{self.keyword}{date_string}{abc[abc_num]}.json'   # new file name

            # double check in case data is overwritten >> if it exists, abc++ until no such file exists
            while (new_file in folder_files) or (new_file in running_files):
                abc_num += 1
                new_file = f'{self.keyword}{date_string}{abc[abc_num]}.json'

            
            

            # final command
            cmd = f'twarc filter "{self.keyword}" > {running_path}{new_file}'

            start_time = datetime.now()
            finish_time = start_time + timedelta(seconds=self.sleep_time)

            print(f'\nProcessing(Start at {start_time.hour}:{start_time.minute}/ Finish at {finish_time.hour}:{finish_time.minute}) --------- {cmd}')
            pro = subprocess.Popen(cmd, stdout=subprocess.PIPE, 
                            shell=True, preexec_fn=os.setsid)

            pid = os.getpgid(pro.pid)

            print(f'\nPID: {pid} or {int(pid)+1} or {int(pid)+2}(if terminated, type "kill {pid}" or "kill {int(pid)+1}" or "kill {int(pid)+2}" to fully exit the execution)\n If killing failed, check "top" and kill the PID manually!!!')

            print('\n (Collecting .......)\n')

            time.sleep(self.sleep_time) # set 4 hours as a batch 

            try:
                os.killpg(pid, signal.SIGTERM)
                print(pid, ' not found')
            except:
                print('1: killing failed')
            
            try:
                os.killpg(int(pid)+1, signal.SIGTERM)
                print('killing ', int(pid)+1)

            except:
                print(int(pid)+1, ' not found')

            cmd_move = f"mv {running_path}{new_file} {folder_path}"
            os.system(cmd_move)

            print(f'\n---------------({new_file})-----------------\n\n')

            


if __name__ == '__main__':
    '''
    keyword --- keyword to filter/ search,
    sleep_time --- sleep time in second; 14400, namely 4 hours
    folder_path ---  path to your depository
    '''
    
    keyword = 'російський'
    sleep_time = 7200
    folder_path = '/home/ml4p/Downloads/peace-pipeline/peace-machine/peacemachine/scripts/' # remeber to add / at the end

    running_path = '/home/ml4p/Downloads/peace-pipeline/peace-machine/peacemachine/scripts/running/'


    auto_pipeline(keyword = keyword, sleep_time = sleep_time, folder_path = folder_path, running_path=running_path)
