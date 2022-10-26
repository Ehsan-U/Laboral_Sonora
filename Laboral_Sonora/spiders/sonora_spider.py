import json
from math import fabs
import os
import pdfplumber
import scrapy
import datetime
from dateutil.rrule import rrule,DAILY
from datetime import date
import re
import io
from scrapy_requests import HtmlRequest
from inline_requests import inline_requests
from http.cookies import SimpleCookie
from unidecode import unidecode
from isodate import parse_datetime, parse_date


def cal_range(start,end, local_db):
    days = []
    s_year, s_month, s_day = map(int,start.split('/'))
    if end:
        e_year, e_month, e_day = map(int,end.split('/'))
        days_range = [day.isoformat()[:10] for day in rrule(DAILY, dtstart=date(s_year, s_month, s_day), until=date(e_year, e_month, e_day))]
    else:
        days_range = [day.isoformat()[:10] for day in rrule(DAILY, dtstart=date(s_year, s_month, s_day), until=date.today())]
    fechas = set()
    local_db.seek(0)
    fechas = local_db.read().split('\n')
    for d in days_range:
        year, month, day = map(int,d.split("-"))
        that_day = datetime.datetime(year, month, day)
        if that_day.weekday() > 4:
            pass
        else:
            d = d.replace('-','/')
            if not d in fechas:
                # print(' [+] Loaded :', d)
                d = datetime.datetime.strptime(d,"%Y/%m/%d").strftime("%d/%m/%Y")
                days.append(d)
            else:
                pass
                # print('\r [+] Already exist in db >', d, end='')
    return days


class SonoraSpiderSpider(scrapy.Spider):
    name = 'sonora_spider'
    allowed_domains = ['sonora.gob.mx']
    headers = {
      'Accept': b'application/xml, text/xml, */*; q=0.01',
      'Accept-Language': b'en-GB,en-US;q=0.9,en;q=0.8',
      'Accept-Encoding': b'gzip, deflate',
      'Content-Type': b'application/x-www-form-urlencoded; charset=UTF-8',
      'Faces-Request': b'partial/ajax',
      'Host': b'st.sonora.gob.mx:8080',
      'Origin': b'http://st.sonora.gob.mx:8080',
      'Referer':b'http://st.sonora.gob.mx:8080/ListaAcuerdos/',
      'X-Requested-With': b'XMLHttpRequest'
    }
    entidad = {
        "Hermosillo".upper():'1',
        "Ciudad Obregon".upper():'2',
        "Nogales".upper():'3',
        "Guaymas".upper():'4',
        "San Luis Rio Colorado".upper():'5',
        "Navojoa".upper():'6',
        unidecode("Puerto Peñasco".upper()):'7',
        "Junta Especial No. 1 Hermosillo".upper():'8'
    }
    post_url = 'http://st.sonora.gob.mx:8080/ListaAcuerdos/faces/index.xhtml'
    counter = 1
    dups = set()

    def start_requests(self):
        if self.resume == 'True':
            if os.path.exists('memory.json'):
                with open('memory.json','r') as f:
                    self.start_Date = datetime.datetime.strptime(json.load(f).get("start"),"%d/%m/%Y").strftime("%Y/%m/%d")
                    print('resuming ',self.start_Date)
            else:
                self.start_Date = self.start_date
        else:
            self.start_Date = self.start_date
        self.end_Date = self.end_date
        self.starter = 'http://st.sonora.gob.mx:8080/ListaAcuerdos/'
        self.local_db = open('local_db.txt', 'a+')
        days = cal_range(self.start_Date, self.end_Date, self.local_db)
        for entidad,n in self.entidad.items():
            for day in days:
                yield scrapy.Request(url=self.starter, callback=self.parse, meta={"cookiejar":self.counter}, errback=self.handle_failure, cb_kwargs={'day':day,'entidad':entidad,'n':n}, dont_filter=True)
                self.counter +=1       

    def parse(self, response, day, entidad, n):
        cookiejar = response.meta.get("cookiejar")
        sel = scrapy.Selector(text=response.text)
        viewstate = sel.xpath("//input[@name='javax.faces.ViewState']/@value").get()
        payload = {
            "javax.faces.partial.ajax":"true",
            "javax.faces.source":"frm-home:j_idt30",
            "javax.faces.partial.execute":"@all",
            "javax.faces.partial.render":"frm-home",
            "frm-home:j_idt30":"frm-home:j_idt30",
            "frm-home":"frm-home",
            "frm-home:omJunta_input":n,
            "frm-home:omJunta_focus":"",
            "frm-home:j_idt19:calFecha_input":str(day),
            "frm-home:j_idt19:txtExpediente":"",
            "frm-home:j_idt19_activeIndex":"0",
            "javax.faces.ViewState":viewstate
        }
        yield scrapy.FormRequest(self.post_url,callback=self.parse_pdf ,meta={"cookiejar":cookiejar}, method='POST', formdata=payload, errback=self.handle_failure, cb_kwargs={'day':day,'entidad':entidad}, dont_filter=True)
        
    def parse_pdf(self, response,day,entidad):
        cookiejar = response.meta.get("cookiejar")
        sel = scrapy.Selector(text=response.text)
        url = response.urljoin(sel.xpath("//object/@data").get())
        yield scrapy.Request(url, callback=self.save_pdf, meta={"cookiejar":cookiejar}, errback=self.handle_failure, cb_kwargs={'day':day,'entidad':entidad}, dont_filter=True)

    def save_pdf(self, response,day,entidad):
        if response.headers.get("Content-Type").decode('utf-8') == 'application/pdf':
            buffer = io.BytesIO()
            buffer.write(response.body)
            try:
                with pdfplumber.open(buffer) as pdf:
                    pages = pdf.pages
                    all_lines = []
                    for page in pages:
                        words = page.extract_text()
                        lines = words.split('\n')
                        for n, line in enumerate(lines):
                            all_lines.append(line)
                    juzgado = self.clean(all_lines[3].strip())
                    fecha = datetime.datetime.strptime(day, "%d/%m/%Y").strftime("%Y/%m/%d")
                    update_juzgado = {'status':False}
                    j = 3
                    while True:
                        if update_juzgado['status']:
                            juzgado = update_juzgado['juzgado']
                        break_loop = False
                        j += 1 # 4
                        regx = re.compile("([0-9/A-Za-z]+)")
                        actor_expediente = self.clean(all_lines[j])
                        expediente,actor  = (self.clean(regx.findall(actor_expediente)[0].strip()), self.clean(" ".join(regx.findall(actor_expediente)[1:]).strip()))
                        j += 2 # 6
                        demandado = self.clean(all_lines[j].strip())
                        acuerdos = ''
                        while True:
                            j+=1
                            # checking doc ends 
                            try:
                                temp = all_lines[j]
                            except IndexError:
                                break_loop = True
                                break
                            else:
                                if all_lines[j][:4].upper() == 'MESA':
                                    # special case
                                    if "." in all_lines[j]:
                                        exp_regx = re.compile(r"(\d{1,5}/[0-9a-zA-Z]{1,5}\s)")
                                        # if its not expediente
                                        try:
                                            exp = exp_regx.search(all_lines[j]).group(1)
                                        except AttributeError:
                                            acuerdos += all_lines[j] + " "
                                            continue
                                        else:
                                            break
                                    else:
                                        update_juzgado['status'] = True
                                        update_juzgado['juzgado'] = self.clean(all_lines[j].strip())
                                        j +=1 # normalize
                                        break
                                elif 'DICTAMENE' in all_lines[j][:9].upper():
                                    update_juzgado['status'] = True
                                    update_juzgado['juzgado'] = self.clean(all_lines[j].strip())
                                    j +=1 # normalize
                                    break
                                elif "CONVENIOS" in all_lines[j][:9].upper():
                                    update_juzgado['status'] = True
                                    update_juzgado['juzgado'] = self.clean(all_lines[j].strip())
                                    j +=1 # normalize
                                    break
                                elif "CONCILIATION" in all_lines[j][:13].upper():
                                    update_juzgado['status'] = True
                                    update_juzgado['juzgado'] = self.clean(all_lines[j].strip())
                                    j +=1 # normalize
                                    break
                                elif "AMPAROS" in all_lines[j][:8].upper():
                                    update_juzgado['status'] = True
                                    update_juzgado['juzgado'] = self.clean(all_lines[j].strip())
                                    j +=1 # normalize
                                    break
                                elif "JUNTA" in all_lines[j][:6].upper():
                                    update_juzgado['status'] = True
                                    update_juzgado['juzgado'] = self.clean(all_lines[j].strip())
                                    j +=1 # normalize
                                    break
                                elif "SECRETARIA" in all_lines[j][:10].upper():
                                    update_juzgado['status'] = True
                                    update_juzgado['juzgado'] = self.clean(all_lines[j].strip())
                                    j +=1 # normalize
                                    break
                                else:
                                    exp_regx = re.compile(r"(\d{1,5}/[0-9a-zA-Z]{1,5}\s)")
                                    # if its not expediente
                                    try:
                                        exp = exp_regx.search(all_lines[j]).group(1)
                                    except AttributeError:
                                        acuerdos += all_lines[j] + " "
                                        continue
                                    else:
                                        break
                        today = datetime.datetime.now()
                        fecha_insercion = parse_datetime(today.isoformat())
                        fecha_tecnica = parse_datetime(f"{today.isoformat()[:10]}T00:00:00")
                        parsed_Data = {
                            "actor": actor,
                            "demandado": demandado,
                            "entidad": entidad,
                            "expediente": expediente,
                            "fecha": fecha,
                            "fuero": 'COMUN',
                            "juzgado": juzgado,
                            "tipo": '',
                            "acuerdos": self.clean(acuerdos.strip().replace('*','').replace('-','')),
                            "monto": '',
                            "fecha_presentacion": '',
                            "actos_reclamados": '',
                            "actos_reclamados_especificos": '',
                            "Naturaleza_procedimiento": '',
                            unidecode("Prestación_demandada"): '',
                            "Organo_jurisdiccional_origen": '',
                            "expediente_origen": '',
                            "materia": 'LABORAL',
                            "submateria": '',
                            "fecha_sentencia": '',
                            "sentido_sentencia": '',
                            "resoluciones": '',
                            "origen": 'JUNTA LOCAL DE CONCILIACION Y ARBITRAJE DEL ESTADO DE SONORA',
                            "fecha_insercion":fecha_insercion,
                            "fecha_tecnica":fecha_tecnica,
                        }
                        if not fecha in self.dups:
                            self.dups.add(fecha)
                            self.local_db.write(f'{fecha}\n')
                        yield parsed_Data

                        with open('memory.json','w') as f:
                            json.dump({'start':day},f)
                        if break_loop:
                            break
                        else:
                            j -= 1 # normalize
                            continue
            
            except Exception as e:
                # print(e)
                pass
        else:
            pass

    def spider_closed(self):
        self.local_db.close()

    def clean(self, string):
        new_string = ''
        for char in string:
            if char.upper() == 'Ñ':
                new_string += char.upper()
            else:
                new_string += unidecode(char).upper()
        return new_string

    def handle_failure(self, failure):
        pass
