Hotfix for /avito 500 error.

Fixes:
1) Injects template globals in avito_module/blueprint.py so Jinja templates can safely check host routes.
2) Replaces current_app.view_functions usage in avito_module/templates/avito/index.html with host_has_view(...).

Copy these files into the project root with replacement.
