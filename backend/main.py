import asyncio
from threading import Thread
import methods.parser as parser 
import methods.api as api

def start_parser(address):
	asyncio.run(parser.main(address))

def start_main():
	asyncio.run(api.main())
 
thread1 = Thread(target=start_parser, args=("EQBcjALtmHwSBCSpDOZ1_emrSQVtJU6J0POZR-ThkZjfXkZs",), daemon=True)
thread1.start()
start_main()
print('script started')