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
            + f"\tType: {self.t}\n"
            + f"\tIndex: {self.index}\n"
            + f"\tTerm: {self.term}\n"
            + f"\tSender: {self.transaction[0]}\n"
            + f"\tReceiver: {self.transaction[1]}\n"
            + f"\tAmount: {self.transaction[2]}\n"
            + ")\n"
        )