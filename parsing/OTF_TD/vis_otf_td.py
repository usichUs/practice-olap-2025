import pandas as pd
import matplotlib.pyplot as plt

# Загрузка данных
df = pd.read_csv('../../csv_files/otf_td.csv', quotechar='"')

# Создание фигуры и оси
fig, ax = plt.subplots(figsize=(18, len(df) * 0.5))

# Скрыть оси
ax.axis('off')

# Построить таблицу
table = ax.table(
    cellText=df.values,
    colLabels=df.columns,
    cellLoc='left',
    loc='center'
)

table.auto_set_font_size(False)
table.set_fontsize(10)
table.auto_set_column_width(col=list(range(len(df.columns))))

# Сохранить как картинку
plt.savefig("otf_td_table.png", bbox_inches='tight', dpi=200)
plt.close()