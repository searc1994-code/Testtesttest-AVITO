Вариант 3: shared UI engine + profile-driven selectors.

Гипотеза:
нужна не разовая правка в price_uploader, а переиспользуемый механизм поиска и клика по WB UI,
который можно потом применять и в других модулях.

Что изменено:
- добавлен новый модуль wb_ui_actions.py
- price_uploader использует общий smart_click engine
- wb_ui_profile.json дополнен блоком automation_prices, чтобы селекторы/тексты можно было править без изменения кода
- приоритет dropdown-опции: 'Цены по размерам'

Менять:
- price_uploader.py
- wb_ui_actions.py
- wb_ui_profile.json
