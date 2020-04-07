Это референсные конвертеры из XMLTV в EPGX и из EPGX в JTV.

Чтобы увидеть краткий список поддерживаемых параметров запустите конвертер с параметром --help

Пример использования:

Скачиваем файл xmltv.xml.gz с www.teleguide.info

Запускаем конвертер в EPGX:

zcat xmltv.xml.gz | ./parser -offset '01-12-2019 09:00' -timespan 9999h -xmap ./map.txt -output schedule.epgx.gz -tz 'Asia/Novosibirsk'

При необходимости, конвертируем полученный файл в JTV:

./jtvgen -offset-time +4 -input schedule.epgx.gz -charset "windows-1251" -output jtv-win1251.zip

Проверка EPGX на целостность:

./epgx schedule.epgx.gz
