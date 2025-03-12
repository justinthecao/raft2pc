

from enum import Enum

class LETypes(Enum):
    INTRA =  "I"
    CROSS = "C"
    CROSSDECISION = "M"
    GHOST = "G"


class logEntry():
    #type of entry
    t = None
    #index of entry
    index = None
    #term of the logentry
    term = None
    #transactions are in the form (sender, receiver, amount)
    transaction = None
    #id of the request
    requestID = None
    def __init__(self, index, term, trans=None, t=None, requestID=None):
        self.t = t
        self.index = index
        self.term = term
        self.transaction = trans  # Transaction is a tuple (sender, receiver, amount)
        self.requestID = requestID

    def to_dict(self):
        """Convert the logEntry object to a dictionary."""
        return {
            "t": self.t,
            "index": self.index,
            "term": self.term,
            "transaction": self.transaction,
            "requestID": self.requestID
        }

    @staticmethod
    def from_dict(data):
        """Create a logEntry object from a dictionary."""
        return logEntry(
            index=data["index"],
            term=data["term"],
            trans=data["transaction"],
            t=data.get("t"),
            requestID=data.get("requestID")
        )

    def __str__(self):
        if self.t == LETypes.CROSSDECISION.value:
            if self.transaction[1] == 1:
                blockType = "CommitCross"
            else:
                blockType = "AbortCross"
            return (
                "(\n"
                + f"\tType: {blockType}\n"
                # + f"\tID: {self.requestID}\n"
                + f"\tIndex: {self.index}\n"
                + f"\tTerm: {self.term}\n"
                + f"\tTransactionIndex: {self.transaction[0]}\n"
                + ")\n"
            )
        elif self.t == LETypes.INTRA.value:
            blockType = "Intra"
            return (
                "(\n"
                + f"\tType: {blockType}\n"
                + f"\tIndex: {self.index}\n"
                + f"\tTerm: {self.term}\n"
                + f"\tSender: {self.transaction[0]}\n"
                + f"\tReceiver: {self.transaction[1]}\n"
                + f"\tAmount: {self.transaction[2]}\n"
                + ")\n"
            )
        elif self.t == LETypes.CROSS.value:
            blockType = "Cross"
            return (
                "(\n"
                + f"\tType: {blockType}\n"
                + f"\tIndex: {self.index}\n"
                + f"\tTerm: {self.term}\n"
                + f"\tSender: {self.transaction[0]}\n"
                + f"\tReceiver: {self.transaction[1]}\n"
                + f"\tAmount: {self.transaction[2]}\n"
                + ")\n"
            )
        elif self.t == LETypes.GHOST.value:
            return "..."
        
       