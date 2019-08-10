import random
from nltk.corpus import words


class RandomSentenceGenerator:
    def __init__(self,vocab=None):
        if(vocab == None):
            self._words = words.words()
        else:
            self._words = vocab
        self._n_words = len(self._words)
    def generate_sentence(self,length):
        sentence = " ".join(random.sample(self._words,length))
        return sentence

class OutputFileStream:
    def dump(self,data,absolute_path):
        fp = open(absolute_path,'w')
        fp.write(data)

def main():
    random_sentence_generator = RandomSentenceGenerator()
    output_file_stream = OutputFileStream()
    for i in range(10000):
        output_file_stream.dump(random_sentence_generator.generate_sentence(5000),"Inputs/ip-"+str(i)+".txt")

main()





