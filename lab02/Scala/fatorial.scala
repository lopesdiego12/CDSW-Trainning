def fatorial(n: Int): Int = n match {
    case 0 => 1
    case _ => n * factorial(n-1)
}

def fatorial_f(n: Int): String = {
      if (n == 0) 
          return "O fatorial de 0 eh = 1"
      else          
          return "O fatorial de " + n + "= " + n * factorial(n-1)
}