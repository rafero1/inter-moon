from asyncio import Event

class RequestWrapper:
    def __init__(self, request):
        self.ready = Event()
        self.request = request
        self.result = None
