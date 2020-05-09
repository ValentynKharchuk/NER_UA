import numpy as np
import tokenize_uk
from keras.models import load_model
from tensorflow.python.keras.backend import set_session
from keras.preprocessing.sequence import pad_sequences


def ner_nlp_extracting(text, model, vesum, word2indx, tag2indx, sess, graph):

    X = list(map(lambda sentence: tokenize_uk.tokenize_words(sentence),
                 tokenize_uk.tokenize_sents(' '.join(tokenize_uk.tokenize_words(text)))))

    X_tokenized = np.array([[word for word in sentence] for sentence in X])
    X = [[word2indx.get(vesum.get_main_form_from_vesum(word), word2indx['UNKNOWN']) for word in sentence] for sentence
         in X]
    X = pad_sequences(X, maxlen=70, padding='post', truncating='post', value=word2indx['ENDPAD'])


    with graph.as_default():
        set_session(sess)
        pred = np.argmax(model.predict(X), axis=-1)

    res = [(sent, list(map(lambda tag: list(filter(lambda key: tag2indx[key] == tag, tag2indx))[0], tags[:len(sent)])))
           for sent, tags in zip(X_tokenized, pred)]

    tokens = list()
    tags = list()

    for tokens_tmp, tags_tmp in res:
        tokens.extend(tokens_tmp)
        tags.extend(tags_tmp)

    find_tags = list()

    start_index = 0
    finish_index = 0

    for ind, tag in enumerate(tags):
        if (ind == 0 or ((ind > 0) and tags[ind - 1] == 'O')) and tag != 'O':
            token = tokens[ind]
            start_index = text.index(token, finish_index)
            finish_index = text.index(token, finish_index) + len(token)
        elif tag != 'O':
            token = tokens[ind]
            finish_index = text.index(token, finish_index) + len(token)
        elif ind > 0 and (tags[ind - 1][0] == 'B' or tags[ind - 1][0] == 'I') and tag == 'O':
            ner = tags[ind - 1][2:]
            ner_dict = dict()
            ner_dict['entity_type'] = ner
            ner_dict['start_index'] = start_index
            ner_dict['finish_index'] = finish_index
            ner_dict['text_entity'] = text[start_index:finish_index]
            find_tags.append(ner_dict)

    return find_tags
