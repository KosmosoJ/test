import re

text = "Нужно заполнить табель на сотрудника Иван Иванов [user_id:13684d1d-233b-4827-8548-f2b3197b8302]"

print(text.replace(f'[{re.search(r'\[(.*?)\]', text).group(1)}]','ID'),)

print(re.search(r'\[(.*?)\]', text).group(1))
# Найти содержимое внутри первых квадратных скобок
# bracket_content = ...
# print(bracket_content)  # Вывод: user_id: 2

# # Разделить основную строку и содержимое скобок
# main_text = re.sub(r'\s*\[.*?\]\s*', '', text).strip()
# print(main_text)  # Вывод: Нужно заполнить табель на сотрудника Иван Иванов

