# Минимальное подключение в host app

В `app.py` после создания Flask app:

```python
from avito_module import register_avito_module
register_avito_module(app)
```

Ссылку в верхнее меню можно добавить так же, как уже сделаны другие разделы:

```jinja2
<a href="{{ url_for('avito_module.avito_index', tenant_id=active_tenant_id) }}">Avito</a>
```

Если хочешь, следующей итерацией я могу уже дать точечный patch под твой `app.py` и конкретный шаблон навигации.
