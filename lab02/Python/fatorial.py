def fatorial(numero):    
    fat = 1
    i = 2
    while i <= numero:
        fat = fat*i
        i = i + 1

    print("O valor de %d! eh =" %numero, fat)

import math    
    
def fatorial_f(numero):
  return math.factorial(numero)