MSG=$1
curl -X POST -H "Authorization: Bearer $LINE_TOKEN" -F "message=$MSG" https://notify-api.line.me/api/notify
