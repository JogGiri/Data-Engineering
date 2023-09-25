def check_prime(n):
    for i in range(2,int(n**0.5)+1):
        if n%i==0:
            return 1 #non prime
    return 0 #prime
    
while True:
    n=int(input("please enter a number: "))
    flag = check_prime(n)
    if flag==1:
        print(f"{n} is a non prime number")
    else:
        print(f"{n} is a prime number")
        a=input("do you want to continue:(y/n): ")
        if a.lower()=='y':
            print("ok continue")
        else:
            print("ok exiting, Thankyou")
            break
    
