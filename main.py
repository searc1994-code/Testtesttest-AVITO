import atexit

import automation_scheduler
import config
from app import app


if __name__ == '__main__':
    started = automation_scheduler.start_scheduler()
    if started:
        atexit.register(automation_scheduler.stop_scheduler)
    app.run(debug=bool(getattr(config, 'FLASK_DEBUG', False)))
