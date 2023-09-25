import time

# def whileloop():
#     i=0
#     while i<10:
#         i=i+1
#         print(i,end=" ")
#         time.sleep(1)

# def forloop():
#     i=0
#     for i in range(10):
#         print(i,end=" ")
#         time.sleep(2)

# init=time.time()
# forloop()
# t1=time.time()-init
# init=time.time()
# whileloop()
# t2=time.time()-init
# print(t1,t2)

#------------------------------------------------------------


t=time.localtime()
print(t)

formated_time= time.strftime("%Y-%m-%d %H:%M:%S",t)
print(formated_time)