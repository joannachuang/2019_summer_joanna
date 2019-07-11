
class url_string:

    def __init__(self, url):  
        import io
        import requests

        self.url = url
        response = requests.get(url , verify=False)    
        self.byteio = io.BytesIO()
        
        for chunk in response.iter_content(chunk_size=512 * 1024): 
            if chunk: # filter out keep-alive new chunks
                self.byteio.write(chunk)

    def to_file(self, filename):
        f = open (filename,'wb')
        f.write( self.byteio.getvalue())
        f.close()
        return 

    def to_str(self) :
        return self.byteio.getvalue() 

    def to_lzma_xz(self) :
        import lzma
        return lzma.compress(self.byteio.getvalue())

    def to_ungzip(self):
        import gzip
        return gzip.decompress(self.byteio.getvalue())
