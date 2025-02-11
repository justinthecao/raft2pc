SHARDID = 3
users = [9001+ (SHARDID*3),9002+ (SHARDID*3),9003+ (SHARDID*3)]
print(users)

class logEntry():
    #type of entry
    t = None
    #index of entry
    index = None
    #term of the logentry
    term = None
    #transactions are in the form (sender, receiver, amount)
    transaction = None
    def __init__(self, index, term, trans=None, t=None):
        self.t = t
        self.index = index
        self.term = term
        self.transaction = trans
    def print(self):
        return "(\n" + "\tIndex: " + int(self.index) + "\n\tTerm: " + int(self.term) + "\n\tSender: " + self.transaction[0] + "\n\tReceiver: " + self.transaction[1] + "\n\tAmount: " + self.transaction[2] + "\n)\n"
    
prevLog = logEntry(index=0,term=0)


data = f"AppendEntries asfdjkl sdaf jklsdaf  ...... a"
split = data.split("...")
print(split)