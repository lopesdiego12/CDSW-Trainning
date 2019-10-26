fatorial <- function(numero) {
  fat <- 1
  i <- 2
  while (i <= numero) {
    fat = fat*i
    i = i + 1    
  }
  resultado = paste("O valor de", numero, "! eh =", fat)
  return(resultado)
}

fatorial_f <- function(numero) {
  resultado <- factorial(numero)
  return(resultado)
}
