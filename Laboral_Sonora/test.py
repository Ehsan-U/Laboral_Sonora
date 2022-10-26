from urllib.parse import urljoin
from requests import ConnectTimeout
from requests_html import HTMLSession
import json
from scrapy.selector import Selector
import time

session = HTMLSession()


def extract_viewstate(session, home_url, proxy=None):
    try:
        resp = session.get(home_url)
        if resp.status_code == 200:
            pass
        else:
            raise ConnectionError
    except (ConnectTimeout, ConnectionError):
        print(" [+] Network Issue detected")
        print(' Retrying after 5 seconds')
        time.sleep(5)
        extract_viewstate(session, home_url)
    else:
        sel = Selector(text=resp.text)
        viewstate = sel.xpath("//input[@name='javax.faces.ViewState']/@value").get()
        return viewstate

base_url = 'http://st.sonora.gob.mx:8080/'
home_url = 'http://st.sonora.gob.mx:8080/ListaAcuerdos/'
post_url = 'http://st.sonora.gob.mx:8080/ListaAcuerdos/faces/index.xhtml'
viewstate = extract_viewstate(session, home_url)
payload = {
    "javax.faces.partial.ajax":"true",
    "javax.faces.source":"frm-home:j_idt30",
    "javax.faces.partial.execute":"@all",
    "javax.faces.partial.render":"frm-home",
    "frm-home:j_idt30":"frm-home:j_idt30",
    "frm-home":"frm-home",
    "frm-home:omJunta_input":'1',
    "frm-home:omJunta_focus":"",
    "frm-home:j_idt19:calFecha_input":"03/01/2022",
    "frm-home:j_idt19:txtExpediente":"",
    "frm-home:j_idt19_activeIndex":"0",
    "javax.faces.ViewState":viewstate
}

resp = session.post(post_url ,data=payload)
sel = Selector(text=resp.text)
pdf_url = urljoin(base_url, (sel.xpath("//object/@data").get()))
print(pdf_url)
resp = session.get(pdf_url)
        
   