## Реализованные методы

### add_notifications

    {
	"request_id": "request_id",
	"message": {
		"action": "add_notification",
		"body": {
			"user_id": "id",
			"senders": [
			],
			"title": "Title",
			"message_template": "Нужно заполнить табель на сотрудника [user_id:2]",
			"message_final": "Нужно заполнить табель на сотрудника Иван Иванов [user_id: 2]",
			"created_at": "2024-02-03T12:00:00Z",
			"is_read": "FALSE", # Ставится по дефолту
			"read_at": "NULL", # Ставится по дефолту
			"metadata": {
				"priority": "high",
				"type": "reminder"
			}
		}
	}
}

### read_notification

    {
	"request_id": "id",
	"message": {
		"action": "read_notification",
		"body": {
			"notification_id": "id"
		        }
	            }
    }

### user_notifications

    {
	"request_id": "id",
	"message": {
		"action": "user_notifications",
		"body": {
			"user_id": "id",
			"is_read": true
		}
	            }
    }


**is_read** Опциональный, если передать True/False выдаст только прочитанные/непрочитанные