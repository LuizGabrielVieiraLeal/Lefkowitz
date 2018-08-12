import codecs
from random import randint

class DbConnection(object):
    def __init__(self, dbPath):
        self.path = dbPath
        self.headers = None
        self.records = []

        try:
            file = open(self.path, 'r')
            print('Conexão estabelecida com o banco de dados...')
            file.close()
        except:
            print('Nao foi possivel estabelecer conexao com o arquivo informado')

    def mountA1(self):
        with codecs.open(self.path, 'r', encoding='utf-8') as file:
            print('Mapeando campos e obtendo registros...')
            flag = 0
            for line in file.readlines():
                if flag == 0:
                    line = 'ID;{}'.format(line)
                    self.headers = line.rstrip()
                    flag += 1
                else:
                    self.records.append(line.rstrip())
            file.close()

        print('Organizando dados...')

        self.path = 'A1.txt'

        with codecs.open(self.path, 'a', encoding='utf-8') as file:
            file.write(self.headers)
            file.write('\n')

            print('Gerando ID para os resgistros...')

            lastIds = []
            while len(lastIds) != len(self.records):
                id = randint(1, len(self.records))
                if id not in lastIds:
                    lastIds.append(id)

            recordPosition = 0
            for record in self.records:
                record = '{};{}\n'.format(lastIds[recordPosition], record)
                file.write(record)
                recordPosition += 1

            file.close()

        print('Montagem do arquivo A1 finalizado')

    def mountA2(self):
        self.records = []

        with codecs.open(self.path, 'r', encoding='utf-8') as file:
            print('Mapeando registros a serem indexados...')

            flag = 0
            for line in file.readlines():
                if flag == 0:
                    line = 'ED;{}'.format(line)
                    self.headers = line.rstrip()
                    flag += 1
                else:
                    self.records.append(line.rstrip())
            file.close()

        print('Incluindo campo de endereco de memoria...')

        self.path = 'A2.txt'

        with codecs.open(self.path, 'a', encoding='utf-8') as file:
            file.write(self.headers)
            file.write('\n')
            edPosition = 1
            for record in self.records:
                record = '{};{}\n'.format(edPosition, record)
                file.write(record)
                edPosition += 1
            file.close()

        print('Montagem do arquivo A2 finalizado')

    def mountA3(self):
        self.records = []

        with codecs.open(self.path, 'r', encoding='utf-8') as file:
            flag = 0
            for line in file.readlines():
                if flag == 0:
                    self.headers = line.rstrip()
                    self.headers = self.headers.split(';')
                    flag += 1
                else:
                    self.records.append(line.rstrip())
            file.close()

        print('Preparando dados para gerar arquivos A3...')

        for i in range(2, len(self.headers)):
            path = 'A3 para {}.txt'.format(self.headers[i])
            header = 'ED;ID;{}'.format(self.headers[i])

            with codecs.open(path, 'a', encoding='utf-8') as file:
                file.write(header)
                file.write('\n')
                for record in self.records:
                    record = record.split(';')
                    record = '{};{};{}'.format(record[0], record[1], record[i])
                    file.write(record)
                    file.write('\n')
                file.close()

        print('Montagem dos arquivos A3 finalizados.')

    def mountA4(self):
        with codecs.open(self.path, 'r', encoding='utf-8') as file:
            line = file.readline()
            self.headers = line.rstrip()
            self.headers = self.headers.split(';')
            file.close()

        print('Preparando dados para gerar arquivos A4...')

        for i in range(2, len(self.headers)):
            self.records = []

            path = 'A3 para {}.txt'.format(self.headers[i])
            header = 'ED;ID;{}'.format(self.headers[i])

            with codecs.open(path, 'r', encoding='utf-8') as file:
                flag = 0
                for line in file.readlines():
                    if flag == 0:
                        flag += 1
                    else:
                        data = {}
                        line = line.rstrip()
                        line = line.split(';')
                        data['ED'] = line[0]
                        data['ID'] = line[1]
                        data[self.headers[i]] = line[2]
                        self.records.append(data)

                sortedRecods = sorted(self.records, key=lambda k: k[self.headers[i]])
                file.close()

                path = 'A4 para {}.txt'.format(self.headers[i])

                with codecs.open(path, 'a', encoding='utf-8') as file:
                    file.write(header)
                    file.write('\n')

                    for record in sortedRecods:
                        file.write('{};{};{}'.format(record['ED'], record['ID'], record[self.headers[i]]))
                        file.write('\n')

                    file.close()

        print('Montagem dos arquivos A4 finalizados')

    def mountA5(self):
        with codecs.open(self.path, 'r', encoding='utf-8') as file:
            line = file.readline()
            self.headers = line.rstrip()
            self.headers = self.headers.split(';')
            file.close()

        print('Preparando dados para gerar arquivos A5...')

        for i in range(2, len(self.headers)):
            self.records = []

            path = 'A5 para {}.txt'.format(self.headers[i])
            header = '{};Qtd;Prim'.format(self.headers[i])

            with codecs.open(path, 'a', encoding='utf-8') as file:
                file.write(header)
                file.write('\n')
                file.close()

            firstRecords = []
            secondaryKeys = []
            allKeys = []

            path = 'A4 para {}.txt'.format(self.headers[i])

            with codecs.open(path, 'r', encoding='utf-8') as file:
                flag = 0
                for line in file.readlines():
                    if flag == 0:
                        flag += 1
                    else:
                        line = line.rstrip()
                        line = line.split(';')
                        if line[2] not in secondaryKeys:
                            firstRecords.append(line[0])
                            secondaryKeys.append(line[2])
                            allKeys.append(line[2])
                        else:
                            allKeys.append(line[2])
                file.close()

            path = 'A5 para {}.txt'.format(self.headers[i])

            with codecs.open(path, 'a', encoding='utf-8') as file:
                relevantPosition = 0

                for key in secondaryKeys:
                    counter = 0
                    for i in range(0, len(allKeys)):
                        if allKeys[i] == key:
                            counter += 1
                    file.write('{};{};{}'.format(key, counter, firstRecords[relevantPosition]))
                    file.write('\n')
                    relevantPosition += 1

                file.close()

        print('Montagem dos arquivos A5 finalizados')

    def mountA6(self):
        with codecs.open(self.path, 'r', encoding='utf-8') as file:
            line = file.readline()
            self.headers = line.rstrip()
            self.headers = self.headers.split(';')
            file.close()

        print('Preparando dados para gerar arquivos A6...')

        for i in range(2, len(self.headers)):
            self.records = []

            path = 'A6 para {}.txt'.format(self.headers[i])
            header = 'ED;ID;{};Prox.{}'.format(self.headers[i], self.headers[i])

            with codecs.open(path, 'w', encoding='utf-8') as file:
                file.write(header)
                file.write('\n')
                file.close()

            path = 'A4 para {}.txt'.format(self.headers[i])

            with codecs.open(path, 'r', encoding='utf-8') as file:
                flag = 0
                for line in file.readlines():
                    if flag == 0:
                        flag += 1
                    else:
                        data = {}
                        line = line.rstrip()
                        line = line.split(';')
                        data['ED'] = line[0]
                        data['ID'] = line[1]
                        data[self.headers[i]] = line[2]
                        data['Prox.{}'.format(self.headers[i])] = None
                        self.records.append(data)

                file.close()

            for record in self.records:
                for item in self.records:
                    if item[self.headers[i]] == record[self.headers[i]] and int(item['ED']) > int(record['ED']):
                        if record['Prox.{}'.format(self.headers[i])] is None:
                            record['Prox.{}'.format(self.headers[i])] = item['ED']


            for record in self.records:
                if record['Prox.{}'.format(self.headers[i])] is None:
                    record['Prox.{}'.format(self.headers[i])] = 0

            path = 'A6 para {}.txt'.format(self.headers[i])

            with codecs.open(path, 'a', encoding='utf-8') as file:
                for record in self.records:
                    line = '{};{};{};{}'.format(record['ED'], record['ID'], record[self.headers[i]], record['Prox.{}'.format(self.headers[i])])
                    file.write(line)
                    file.write('\n')
                file.close()

        print('Montagem dos arquivos A6 finalizados')

    def mountA7(self):
        with codecs.open(self.path, 'r', encoding='utf-8') as file:
            line = file.readline()
            self.headers = line.rstrip()
            self.headers = self.headers.split(';')
            file.close()

        print('Preparando dados para gerar arquivos A7...')

        for i in range(2, len(self.headers)):
            self.records = []

            path = 'A7 para {}.txt'.format(self.headers[i])
            header = 'ED;ID;{};Prox.{}'.format(self.headers[i], self.headers[i])

            with codecs.open(path, 'w', encoding='utf-8') as file:
                file.write(header)
                file.write('\n')
                file.close()

            path = 'A6 para {}.txt'.format(self.headers[i])

            with codecs.open(path, 'r', encoding='utf-8') as file:
                flag = 0
                for line in file.readlines():
                    if flag == 0:
                        flag += 1
                    else:
                        data = {}
                        line = line.rstrip()
                        line = line.split(';')
                        data['ED'] = line[0]
                        data['ID'] = line[1]
                        data[self.headers[i]] = line[2]
                        data['Prox.{}'.format(self.headers[i])] = line[3]
                        self.records.append(data)

                sortedRecods = sorted(self.records, key=lambda k: int(k['ID']))
                file.close()

            path = 'A7 para {}.txt'.format(self.headers[i])

            with codecs.open(path, 'a', encoding='utf-8') as file:
                for record in sortedRecods:
                    file.write('{};{};{};{}'.format(record['ED'], record['ID'], record[self.headers[i]], record['Prox.{}'.format(self.headers[i])]))
                    file.write('\n')
                file.close()

        print('Montagem dos arquivos A7 finalizados')

    def mountA8(self):
        finalData = []

        print('Preparando dados para gerar novo arquivo mestre...')

        with codecs.open('A1.txt', 'r', encoding='utf-8') as file:
            line = file.readline()
            self.headers = line.rstrip()
            header = self.headers
            self.headers = self.headers.split(';')
            file.close()

        with codecs.open('A1.txt', 'r', encoding='utf-8') as file:
            flag = 0
            for line in file.readlines():
                if flag == 0:
                    flag += 1
                else:
                    data = {}
                    line = line.rstrip()
                    line = line.split(';')
                    for i in range(0, len(self.headers)):
                        data[self.headers[i]] = line[i]
                    finalData.append(data)
            file.close()

        sortedData = sorted(finalData, key=lambda k: int(k['ID']))

        for i in range(1, len(self.headers)):
            header += ';Prox.{}'.format(self.headers[i])

            with codecs.open('A7 para {}.txt'.format(self.headers[i]), 'r', encoding='utf-8') as file:
                index = 0
                flag = 0
                for line in file.readlines():
                    if flag == 0:
                        flag += 1
                    else:
                        line = line.rstrip()
                        line = line.split(';')
                        sortedData[index]['Prox.{}'.format(self.headers[i])] = line[3]
                        index += 1
                file.close()

        path = 'A8.txt'

        with codecs.open(path, 'w', encoding='utf-8') as file:
            file.write(header)
            file.write('\n')
            file.close()

        header = header.split(';')

        with codecs.open(path, 'a', encoding='utf-8') as file:
            for data in sortedData:
                line = ''
                for i in range(0, len(header)):
                    line += data[header[i]]
                    if i != len(header) - 1:
                        line += ';'
                file.write(line)
                file.write('\n')

        print('Novo arquivo mestre finalizado.')