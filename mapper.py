import sys
from pymystem3 import Mystem
m = Mystem() # создаем объект лемматизатора

inputfile = sys.stdin  # принимаем данные из считанного сырого файла в консоли
next(inputfile)  # пропускаем заголовок
for line in inputfile:
    phrase = line.split(',')[0]  # получаем текст запроса без числа упоминаний
    phrase = m.lemmatize(phrase.strip())[:-1] # приводим слова запроса к 1 словоформе и отбрасываем перенос строки
    for word in phrase:  # еще раз делим фразу на отдельные слова
        if word.isalpha():  # если слово состоит из букв
            print(f'{word}')  # выводим в печать