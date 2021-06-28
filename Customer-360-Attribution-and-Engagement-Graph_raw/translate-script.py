import json

translateJsonFileName = 'translation.json'
translateFiles = [
  {'key': 'zh-Hans', 'json': {}},
  {'key': 'en-US', 'json': {}}
]

if __name__ == "__main__":
  with open('./' + translateJsonFileName, 'r', encoding='utf-8') as f1:
    translateJson = json.loads(f1.read())

  for i in translateFiles:
    for j in translateJson.keys():
      if (i['key'] == 'en-US'):
        i['json'][j] = j
      else:
        i['json'][j] = translateJson[j][i['key']]
    with open('../' + i['key'] + '.json', 'w', encoding='utf-8') as f2:
      f2.write(json.dumps(i['json'], ensure_ascii=False))
