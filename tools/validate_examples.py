"""Проверка примеров сообщений docs/examples/*.json по схеме docs/schemas/message.schema.json.

Дополнительно проверяется, что message_id – это UUID v5, вычисленный по правилу шлюза.
Запуск: python tools/validate_examples.py   (нужен пакет jsonschema)
"""
import json
import sys
import uuid
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

ROOT = Path(__file__).resolve().parent.parent
NAMESPACE = uuid.uuid5(uuid.NAMESPACE_DNS, 'transit-monitor.local')


def expected_id(msg):
    p = msg['payload']
    if 'cause_message_id' in p:   # событие, сформированное сервисом обработки
        return uuid.uuid5(NAMESPACE, f"{p['cause_message_id']}:{p['incident_type']}")
    return uuid.uuid5(NAMESPACE, f"{msg['source_id']}:{msg['seq']}")


def main():
    schema = json.loads((ROOT / 'docs/schemas/message.schema.json').read_text(encoding='utf-8'))
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    failed = 0
    for path in sorted((ROOT / 'docs/examples').glob('*.json')):
        msg = json.loads(path.read_text(encoding='utf-8'))
        errors = [e.message for e in validator.iter_errors(msg)]
        if str(expected_id(msg)) != msg['message_id']:
            errors.append(f'message_id не совпадает с UUID v5 ({expected_id(msg)})')
        if msg['type'] != path.name.split('.json')[0].rsplit('.', 1)[0] and not path.name.startswith(msg['type']):
            errors.append('тип сообщения не совпадает с именем файла')
        print(f"{path.name}: {'OK' if not errors else '; '.join(errors)}")
        failed += bool(errors)
    # отрицательные проверки: схема должна отвергать некорректные сообщения
    bad = json.loads((ROOT / 'docs/examples/vehicle.position.json').read_text(encoding='utf-8'))
    for desc, mutate in (
            ('нет message_id', lambda m: m.pop('message_id')),
            ('неизвестный тип', lambda m: m.update(type='vehicle.speed')),
            ('широта вне диапазона', lambda m: m['payload'].update(lat=123.0)),
            ('лишнее поле', lambda m: m['payload'].update(driver='Иванов'))):
        m = json.loads(json.dumps(bad))
        mutate(m)
        rejected = not validator.is_valid(m)
        print(f'отрицательная проверка «{desc}»: {"отвергнуто" if rejected else "ПРИНЯТО (ошибка)"}')
        failed += not rejected
    sys.exit(1 if failed else 0)


if __name__ == '__main__':
    main()
