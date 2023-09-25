n=int(input("entera number: "))
prime=[]
non_prime=[0,1]
def prime_in_range(n):
    for i in range(2,n+1):
        for j in range(2,int(i**0.5)+1):
            if i%j==0:
                non_prime.append(i)
                break
        else:
            prime.append(i)
            

prime_in_range(n)
print("non_prime:",non_prime)
print("prime:",prime)
