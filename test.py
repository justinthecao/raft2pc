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
        self.transaction = trans  # Transaction is a tuple (sender, receiver, amount)

    def to_dict(self):
        """Convert the logEntry object to a dictionary."""
        return {
            "t": self.t,
            "index": self.index,
            "term": self.term,
            "transaction": self.transaction
        }

    @staticmethod
    def from_dict(data):
        """Create a logEntry object from a dictionary."""
        return logEntry(
            index=data["index"],
            term=data["term"],
            trans=data["transaction"],
            t=data.get("t")
        )

    def __str__(self):
        return (
            "(\n"
            + f"\tIndex: {self.index}\n"
            + f"\tTerm: {self.term}\n"
            + f"\tSender: {self.transaction[0]}\n"
            + f"\tReceiver: {self.transaction[1]}\n"
            + f"\tAmount: {self.transaction[2]}\n"
            + ")\n"
        )
        
prevLog = logEntry(index=0,term=0)


data = f"AppendEntries asfdjkl sdaf jklsdaf  ...... a"
split = data.split("...")
print(split)

for i in range(9):
    print(i)

import json

print(json.loads("""[{"t": "intra", "index": 1, "term": 1, "transaction": [1, 2, 3]}]"""))

