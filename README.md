# mygrep

Распределённый grep на Go. Ищет по файлу или stdin, как обычный grep. В многосерверном режиме файл режется на фрагменты, узлы обрабатывают их параллельно. Результат готов, когда успешно отработали `N/2+1` серверов.

## Требования

- [Go](https://go.dev/dl/) 1.25+
- системный `grep` в PATH (для сравнительных тестов)

## Запуск

1. Клонировать репозиторий:

```bash
git clone https://github.com/sonni-a/distributed-grep.git
cd distributed-grep
```

1. Собрать бинарник:

```bash
go build -o mygrep ./cmd/mygrep
```

## Локальный режим

```bash
./mygrep error examples/sample.txt # ищем слово error в файле, выводятся строки с error
./mygrep -n -i warning examples/sample.txt # ищем warning, не учитывая регистр и с номерами строк
cat examples/sample.txt | ./mygrep -F "error:"  # stdin: ищем текст error: (не регулярное выражение)
```


| Флаг | Значение                   |
| ---- | -------------------------- |
| `-i` | без регистра               |
| `-v` | инверсия                   |
| `-n` | номера строк               |
| `-F` | искать как текст, не regex |


## Кластер

Три терминала с серверами, затем клиент. Если выключить 1 узел, поиск продолжится, если 2, то кворум не выполнится и работа прекратится.

```bash
PEERS=localhost:8081,localhost:8082,localhost:8083

./mygrep server -addr :8081 -id node-1 -advertise localhost:8081 -peers $PEERS
./mygrep server -addr :8082 -id node-2 -advertise localhost:8082 -peers $PEERS
./mygrep server -addr :8083 -id node-3 -advertise localhost:8083 -peers $PEERS

./mygrep -distributed -servers $PEERS -n error examples/sample.txt
```

## Тесты

```bash
go test ./...
```

## Структура

```
distributed-grep/
├── cmd/mygrep/          # CLI: флаги, server / поиск
├── internal/
│   ├── grep/            # поиск по строкам
│   ├── protocol/        # JSON, шарды, кворум N/2+1
│   ├── client/          # локальный и distributed-клиент
│   └── server/          # HTTP-узел, координация
├── tests/               # сравнение с grep, интеграция кластера
├── examples/sample.txt
└── README.md
```

