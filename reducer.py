import sys
from pymystem3 import Mystem

m = Mystem()
counter = {}  # заводим пустой словарь
inputfile = sys.stdin
for line in inputfile:  # принимаем данные из консоли
    line = line.strip()  # отбрасываем пробельные символы с краев
    line = m.lemmatize(line)[0]  # приводим слово к простой форме, чтобы исключить влияние падежей и склонений
    if line != '' and line in counter:  # если такой ключ уже есть в словаре, прибавляем 1
        counter[line] += 1
    else:  # иначе создаем новый ключ
        counter[line] = 1
for key, val in counter.items():
    print(f'слово {key} встретилось {val} раз')  # выводим подсчеты слов на печать