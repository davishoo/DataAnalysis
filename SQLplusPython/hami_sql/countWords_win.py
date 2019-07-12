import logging
import re
import pandas as pd

logging.basicConfig(level=logging.DEBUG, format=' %(asctime)s - %(levelname)s - %(message)s')
logging.disable(logging.CRITICAL)
logging.debug('Start of program\n\n')

file = r'C:\Users\deand\pythonScripts\countWords\Voc.txt'
with open(file, encoding='utf-8-sig') as f:   # check my note for info about utf-8-sig
    text = f.read()

logging.debug('text = :\n%s'%(text))  # check point 1

pattern = r'[(),:;%/.\d+–-]'  # gonna remove unnecessary characters
regex = re.compile(pattern)

textStrip = regex.sub('', text)

logging.debug('textStrip = : \n%s' % textStrip)    # check point 2

unique = set(textStrip.lower().split())

logging.debug('unique = : \n%s'%(unique))  # check point 3

vocList = list(unique)  # voc stands for 'vocabulary'

vocList.sort()

output = pd.Series(vocList, name='Vocabulary')

logging.debug('output = : \n%s'%(output))  # check point 4

df = pd.DataFrame(output)

countList = list(textStrip.lower().split())
frequency = pd.Series(countList)
cnt = frequency.value_counts()    # get every word's frequency

df['Frequency'] = df['Vocabulary'].map(cnt)  # link single word to its frequency
df.to_excel('vocabulary.xlsx', index=False)


