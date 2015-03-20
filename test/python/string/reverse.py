'''
    created on 2014-10-20
    @author : luopan
'''

class MyString():
    def reverse(self, some_seq):
        """
            input : Sequence
            output : Sequence:
                reversed version
        """
        return some_seq[::-1]

    def count_vowels(self, string):
        """
            @param string : String
            @return : Integer :
                No. of vowels or 0
        """
        vowel = "aeiou"
        count = {char:0 for char in vowels}
        for char in string:
            if char in vowels:
                count[char]+=1
        return count

    def is_palindrome(self,some_seq):
        """
            @param some_seq: sequence of anything
            @return:         Boolean:
                        palindrome check of sequence passed 
        """
        return some_seq == self.reverse(some_seq)

    def count_words(self,string=None,file=None):
        """
            @param string : A string
            @param file : A file to be read
        """
        word_count = 0
        if string:
            word_count  = len(string.split())
        if file:
            with open(file) as f:
                word_count = len(f.read().split())
        return word_count

    def piglatin(self, string):
        """
            Pig Latin – Pig Latin is a game of alterations played on the English language game. 
            To create the Pig Latin form of an English word the initial consonant sound is 
            transposed to the end of the word and an ay is affixed 
            (Ex.: "banana" would yield anana-bay). Read Wikipedia for more information on rules.
        """
        words = []
        vowels = 'aeiou'
        for word in string.split():
            if len(word) > 2 and word[0] not in vowels:
                words.append(word[1:]+'-'+word[0]+'ay')
            else:
                words.append(word+'-ay')
        return ' '.join(words)

if __name__ == '__main__':
    x = MyString()
    string = "a barbie vanquished the knights of the round table by hitting them in the face"
    print(x.piglatin(string))
