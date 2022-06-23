import sys

prev = None
cnt = 0
inputfile = sys.stdin  # принимаем данные из консоли
for line in inputfile:
    line = line.strip()  # отбрасываем пробельные символы с краев
    if prev:  # если слово не пустое
        if prev == line:  # так же проверяем равно ли оно текущему
            cnt += 1  # добавляем к счетчику 1
        else:
            print(f'слово {prev} встретилось {cnt} раз') # если не расно выводим накопленое на печать
            prev = line # обновляем слово и счетчик
            cnt = 1
    else:
        prev = line  # если пустое, присваиваем первое слово
        cnt = 1
print(f'слово {prev} встретилось {cnt} раз')  # выводим подсчеты слов на печать