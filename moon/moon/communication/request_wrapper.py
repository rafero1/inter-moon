class RequestWrapper:
    def __init__(self, request):
        self.ready = False
        self.request = request
        self.result = None
