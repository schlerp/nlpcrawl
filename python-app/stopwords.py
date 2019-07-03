stop_words = ["a", "about", "above", "after", "again", "against", "ain", "all", "am", 
              "an", "and", "any", "are", "aren", "aren't", "as", "at", "be", "because",
              "been", "before", "being", "below", "between", "both", "but", "by", "can",
              "couldn", "couldn't", "d", "did", "didn", "didn't", "do", "does", "doesn",
              "doesn't", "doing", "don", "don't", "down", "during", "each", "few", "for",
              "from", "further", "had", "hadn", "hadn't", "has", "hasn", "hasn't", "have",
              "haven", "haven't", "having", "he", "her", "here", "hers", "herself", "him", 
              "himself", "his", "how", "i", "if", "in", "into", "is", "isn", "isn't", "it", 
              "it's", "its", "itself", "just", "ll", "m", "ma", "me", "mightn", "mightn't", 
              "more", "most", "mustn", "mustn't", "my", "myself", "needn", "needn't", "no", 
              "nor", "not", "now", "o", "of", "off", "on", "once", "only", "or", "other", 
              "our", "ours", "ourselves", "out", "over", "own", "re", "s", "same", "shan", 
              "shan't", "she", "she's", "should", "should've", "shouldn", "shouldn't", "so",
               "some", "such", "t", "than", "that", "that'll", "the", "their", "theirs", 
               "them", "themselves", "then", "there", "these", "they", "this", "those", 
               "through", "to", "too", "under", "until", "up", "ve", "very", "was", "wasn", 
               "wasn't", "we", "were", "weren", "weren't", "what", "when", "where", "which", 
               "while", "who", "whom", "why", "will", "with", "won", "won't", "wouldn", 
               "wouldn't", "y", "you", "you'd", "you'll", "you're", "you've", "your", 
               "yours", "yourself", "yourselves", "could", "he'd", "he'll", "he's", 
               "here's", "how's", "i'd", "i'll", "i'm", "i've", "let's", "ought", 
               "she'd", "she'll", "that's", "there's", "they'd", "they'll", "they're", 
               "they've", "we'd", "we'll", "we're", "we've", "what's", "when's", "where's", 
               "who's", "why's", "would", "able", "abst", "accordance", "according", 
               "accordingly", "across", "act", "actually", "added", "adj", "affected", 
               "affecting", "affects", "afterwards", "ah", "almost", "alone", "along", 
               "already", "also", "although", "always", "among", "amongst", "announce", 
               "another", "anybody", "anyhow", "anymore", "anyone", "anything", "anyway", 
               "anyways", "anywhere", "apparently", "approximately", "arent", "arise", "around", 
               "aside", "ask", "asking", "auth", "available", "away", "awfully", "b", "back", 
               "became", "become", "becomes", "becoming", "beforehand", "begin", "beginning", 
               "beginnings", "begins", "behind", "believe", "beside", "besides", "beyond", 
               "biol", "brief", "briefly", "c", "ca", "came", "cannot", "can't", "cause", 
               "causes", "certain", "certainly", "co", "com", "come", "comes", "contain", 
               "containing", "contains", "couldnt", "date", "different", "done", "downwards",
               "due", "e", "ed", "edu", "effect", "eg", "eight", "eighty", "either", "else", 
               "elsewhere", "end", "ending", "enough", "especially", "et", "etc", "even", 
               "ever", "every", "everybody", "everyone", "everything", "everywhere", "ex", 
               "except", "f", "far", "ff", "fifth", "first", "five", "fix", "followed", 
               "following", "follows", "former", "formerly", "forth", "found", "four", 
               "furthermore", "g", "gave", "get", "gets", "getting", "give", "given", 
               "gives", "giving", "go", "goes", "gone", "got", "gotten", "h", "happens", 
               "hardly", "hed", "hence", "hereafter", "hereby", "herein", "heres", "hereupon", 
               "hes", "hi", "hid", "hither", "home", "howbeit", "however", "hundred", "id", 
               "ie", "im", "immediate", "immediately", "importance", "important", "inc", 
               "indeed", "index", "information", "instead", "invention", "inward", "itd", 
               "it'll", "j", "k", "keep", "keeps", "kept", "kg", "km", "know", "known", 
               "knows", "l", "largely", "last", "lately", "later", "latter", "latterly", 
               "least", "less", "lest", "let", "lets", "like", "liked", "likely", "line", 
               "little", "'ll", "look", "looking", "looks", "ltd", "made", "mainly", "make", 
               "makes", "many", "may", "maybe", "mean", "means", "meantime", "meanwhile", 
               "merely", "mg", "might", "million", "miss", "ml", "moreover", "mostly", "mr", 
               "mrs", "much", "mug", "must", "n", "na", "name", "namely", "nay", "nd", "near", 
               "nearly", "necessarily", "necessary", "need", "needs", "neither", "never", 
               "nevertheless", "new", "next", "nine", "ninety", "nobody", "non", "none", 
               "nonetheless", "noone", "normally", "nos", "noted", "nothing", "nowhere", 
               "obtain", "obtained", "obviously", "often", "oh", "ok", "okay", "old", "omitted", 
               "one", "ones", "onto", "ord", "others", "otherwise", "outside", "overall", 
               "owing", "p", "page", "pages", "part", "particular", "particularly", "past", 
               "per", "perhaps", "placed", "please", "plus", "poorly", "possible", "possibly", 
               "potentially", "pp", "predominantly", "present", "previously", "primarily", 
               "probably", "promptly", "proud", "provides", "put", "q", "que", "quickly", "quite", 
               "qv", "r", "ran", "rather", "rd", "readily", "really", "recent", "recently", "ref", 
               "refs", "regarding", "regardless", "regards", "related", "relatively", "research", 
               "respectively", "resulted", "resulting", "results", "right", "run", "said", "saw", 
               "say", "saying", "says", "sec", "section", "see", "seeing", "seem", "seemed", 
               "seeming", "seems", "seen", "self", "selves", "sent", "seven", "several", "shall", 
               "shed", "shes", "show", "showed", "shown", "showns", "shows", "significant",
               "significantly", "similar", "similarly", "since", "six", "slightly", "somebody", 
               "somehow", "someone", "somethan", "something", "sometime", "sometimes", 
               "somewhat", "somewhere", "soon", "sorry", "specifically", "specified", "specify", 
               "specifying", "still", "stop", "strongly", "sub", "substantially", "successfully", 
               "sufficiently", "suggest", "sup", "sure", "take", "taken", "taking", "tell", 
               "tends", "th", "thank", "thanks", "thanx", "thats", "that've", "thence", 
               "thereafter", "thereby", "thered", "therefore", "therein", "there'll", 
               "thereof", "therere", "theres", "thereto", "thereupon", "there've", "theyd", 
               "theyre", "think", "thou", "though", "thoughh", "thousand", "throug", "throughout", 
               "thru", "thus", "til", "tip", "together", "took", "toward", "towards", "tried", 
               "tries", "truly", "try", "trying", "ts", "twice", "two", "u", "un", 
               "unfortunately", "unless", "unlike", "unlikely", "unto", "upon", "ups", 
               "us", "use", "used", "useful", "usefully", "usefulness", "uses", "using", 
               "usually", "v", "value", "various", "'ve", "via", "viz", "vol", "vols", 
               "vs", "w", "want", "wants", "wasnt", "way", "wed", "welcome", "went", 
               "werent", "whatever", "what'll", "whats", "whence", "whenever", "whereafter", 
               "whereas", "whereby", "wherein", "wheres", "whereupon", "wherever", "whether", 
               "whim", "whither", "whod", "whoever", "whole", "who'll", "whomever", "whos", 
               "whose", "widely", "willing", "wish", "within", "without", "wont", "words", 
               "world", "wouldnt", "www", "x", "yes", "yet", "youd", "youre", "z", "zero", 
               "a's", "ain't", "allow", "allows", "apart", "appear", "appreciate", "appropriate", 
               "associated", "best", "better", "c'mon", "c's", "cant", "changes", "clearly", 
               "concerning", "consequently", "consider", "considering", "corresponding", "course", 
               "currently", "definitely", "described", "despite", "entirely", "exactly", 
               "example", "going", "greetings", "hello", "help", "hopefully", "ignored", 
               "inasmuch", "indicate", "indicated", "indicates", "inner", "insofar", "it'd", 
               "keep", "keeps", "novel", "presumably", "reasonably", "second", "secondly", 
               "sensible", "serious", "seriously", "sure", "t's", "third", "thorough", 
               "thoroughly", "three", "well", "wonder", "a", "about", "above", "above", 
               "across", "after", "afterwards", "again", "against", "all", "almost", "alone", 
               "along", "already", "also", "although", "always", "am", "among", "amongst", 
               "amoungst", "amount", "an", "and", "another", "any", "anyhow", "anyone", 
               "anything", "anyway", "anywhere", "are", "around", "as", "at", "back", "be", 
               "became", "because", "become", "becomes", "becoming", "been", "before", 
               "beforehand", "behind", "being", "below", "beside", "besides", "between", 
               "beyond", "bill", "both", "bottom", "but", "by", "call", "can", "cannot", 
               "cant", "co", "con", "could", "couldnt", "cry", "de", "describe", "detail", 
               "do", "done", "down", "due", "during", "each", "eg", "eight", "either", 
               "eleven", "else", "elsewhere", "empty", "enough", "etc", "even", "ever", 
               "every", "everyone", "everything", "everywhere", "except", "few", "fifteen", 
               "fify", "fill", "find", "fire", "first", "five", "for", "former", "formerly", 
               "forty", "found", "four", "from", "front", "full", "further", "get", "give", 
               "go", "had", "has", "hasnt", "have", "he", "hence", "her", "here", "hereafter", 
               "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his", 
               "how", "however", "hundred", "ie", "if", "in", "inc", "indeed", "interest", 
               "into", "is", "it", "its", "itself", "keep", "last", "latter", "latterly", 
               "least", "less", "ltd", "made", "many", "may", "me", "meanwhile", "might", 
               "mill", "mine", "more", "moreover", "most", "mostly", "move", "much", "must", 
               "my", "myself", "name", "namely", "neither", "never", "nevertheless", "next", 
               "nine", "no", "nobody", "none", "noone", "nor", "not", "nothing", "now", 
               "nowhere", "of", "off", "often", "on", "once", "one", "only", "onto", "or", 
               "other", "others", "otherwise", "our", "ours", "ourselves", "out", "over", 
               "own", "part", "per", "perhaps", "please", "put", "rather", "re", "same", 
               "see", "seem", "seemed", "seeming", "seems", "serious", "several", "she", 
               "should", "show", "side", "since", "sincere", "six", "sixty", "so", "some", 
               "somehow", "someone", "something", "sometime", "sometimes", "somewhere", 
               "still", "such", "system", "take", "ten", "than", "that", "the", "their", 
               "them", "themselves", "then", "thence", "there", "thereafter", "thereby", 
               "therefore", "therein", "thereupon", "these", "they", "thickv", "thin", 
               "third", "this", "those", "though", "three", "through", "throughout", 
               "thru", "thus", "to", "together", "too", "top", "toward", "towards", 
               "twelve", "twenty", "two", "un", "under", "until", "up", "upon", "us", 
               "very", "via", "was", "we", "well", "were", "what", "whatever", "when", 
               "whence", "whenever", "where", "whereafter", "whereas", "whereby", "wherein", 
               "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whoever", 
               "whole", "whom", "whose", "why", "will", "with", "within", "without", "would", 
               "yet", "you", "your", "yours", "yourself", "yourselves", "the"]