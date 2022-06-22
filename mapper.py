import sys
inputfile = sys.stdin  # принимаем данные из считанного сырого файла в консоли
next(inputfile)  # пропускаем заголовок
for line in inputfile:
    phrase = line.split(',')[0]  # получаем текст запроса без числа упоминаний
    for word in phrase.strip().split(' '):  # еще раз делим фразу на отдельные слова
        if word.isalpha():  # если слово состоит из букв
            print(f'{word}')  # выводим в печать