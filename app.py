from __future__ import unicode_literals

import pandas as pd
import os
import io

from sqlalchemy.engine import Engine
from xlwt import Workbook

from flask import Flask, render_template, url_for, request, redirect
from sqlalchemy import create_engine, inspect, MetaData, Table
from werkzeug.utils import secure_filename
from sqlalchemy.orm import sessionmaker
from time import sleep

UPLOAD_FOLDER = os.path.join(os.getcwd(), 'files')

app = Flask(__name__)
app.config['DEBUG'] = True
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


ALLOWED_EXTENSIONS = {'xlsx'}


def get_table(tabel):
    engine = create_engine('sqlite:///MBRO.db')
    table_meta = MetaData(engine)
    table = Table(tabel, table_meta, autoload=True)
    DBSession = sessionmaker(bind=engine)
    session = DBSession()
    return session, table


def verifica_fisier(fila):
    return fila.split('.')[-1].lower() in ALLOWED_EXTENSIONS


def transforma_NIR(fila):
    """
    Din fisier xlsx se creeaza dataframe
    """
    print('INCEPE TRANSFORMA NIR')
    if 'files' not in os.getcwd():
        os.chdir(os.getcwd() + os.sep + 'files')
    print(os.getcwd())

    df = pd.read_excel(fila, skiprows=7, skipfooter=11)
    df.dropna(how='all', axis=0, inplace=True)
    df.dropna(how='all', axis=1, inplace=True)
    df.columns = df.columns.str.strip()
    lista_coloane = ['Cod articol furnizor', 'Cod art. SAP', 'Lot', 'Cantitate', 'U.M']
    for x in df.columns.to_list():
        if x not in lista_coloane:
            df.drop(columns=x, inplace=True)
    df.dropna(how='all', inplace=True)
    df.rename(columns={'Cod articol furnizor':'codoe', 'Cod art. SAP':'codsap', 'Lot':'lot', 'Cantitate': 'cant', 'U.M':'um'}, inplace=True)
    for x in df.columns:
        df[x] = df[x].str.strip()
    df.reset_index(inplace=True)
    df.drop(columns='index', inplace=True)
    descrieri = df.loc[df.cant=='X'].codoe.to_list()
    indextodrop = []
    for i in range(len(df.index)):
        if str(df.iloc[i,3]) == 'X' or 'Cantitate' in str(df.iloc[i,3]):
            indextodrop.append(i)
    df.drop(index=indextodrop, inplace=True)
    df.insert(loc=1, column='descriere', value=descrieri)

    df['status'] = '0'
    df.columns = df.columns.str.lower()
    os.remove(fila)
    if 'files' in os.getcwd():
        os.chdir(os.sep.join(os.getcwd().split(os.sep)[:-1]))
    return df


def save_NIR_to_db(fila):
    print("INCEPE SAVE NIR")
    print(os.getcwd())
    # prelucreaza NIR-ul uploadat
    # df = transforma_NIR(fila)
    try:
        df = transforma_NIR(fila)
    except:
        # repara_xls(fila)
        # df = transforma_NIR(fila)
        print("Eroare")

    # salveaza in baza de date NIR-ul
    table_name = "NIR_" + fila.split('.')[0]

    df.to_sql(name=table_name,
              con=create_engine('sqlite:///MBRO.db'),
              if_exists='replace',
              index=False)


def get_tables_from_db():
    engine = create_engine('sqlite:///MBRO.db')
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    return tables


@app.route('/', methods=['GET', 'POST'])
def home():
    tables = get_tables_from_db()

    if request.method == 'POST':
        if 'fisier' not in request.files:
            print('Fara fisier incarcat')
            mesaj = 'Nu s-a incarcat nici un fisier!'
            return render_template('home.html', mesaj=mesaj)
        fisier = request.files['fisier']
        if fisier.filename == '':
            mesaj = 'Fisierul nu are nici un nume.\nIncarca un fisier corespunzator!'
            return render_template('home.html', mesaj=mesaj)
        if not verifica_fisier(fisier.filename):
            mesaj = 'Fisierul nu este in formatul solicitat.\nIncarca doar fisier excel (xlsx)'
            return render_template('home.html', mesaj=mesaj)
        else:
            nume_fisier = secure_filename(fisier.filename)
            print(nume_fisier)
            # creeaza folderul FILES, daca nu este prezent deja
            # pentru upload NIR exportat din SAP
            root_folder = os.getcwd()
            if 'files' not in os.listdir():
                os.mkdir('files')
            os.chdir(os.getcwd() + os.sep + 'files')
            fisier.save(os.path.join(app.config['UPLOAD_FOLDER'], nume_fisier))
            os.chdir(os.sep.join(os.getcwd().split(os.sep)[:-1]))
            save_NIR_to_db(nume_fisier)
            nume_fisier = "NIR_" + nume_fisier.split('.')[0]
            return redirect(url_for('scanare', fila=nume_fisier))

    tables = tables[:10]

    return render_template('home.html', tables=tables)


@app.route('/scanare/<fila>', methods=['GET', 'POST'])
def scanare(fila):
    session, table = get_table(fila)

    mesaj = None
    if request.method == 'POST':
        codoe = request.form['codoe'].strip()
        lot = request.form['lot'].strip()
        row = session.query(table).filter(table.columns.lot == lot).first()
        if row:
            if (row.codoe == codoe or codoe in row.descriere) and float(row.cant.strip()) == float(request.form['cant']):
                session.query(table).filter(table.columns.lot == lot).update({table.columns.status: '1'},
                                                                             synchronize_session=False)
                session.commit()
                mesaj = 'OK'
            elif row.cant != request.form['cant']:
                mesaj = 'Cantitatea nu corespunde!'
            else:
                mesaj = 'Codul OE nu corespunde lotului scanat!'
        else:
            mesaj = 'Lotul nu apartine acestui NIR!'

    nescanate = session.query(table).filter(table.columns.status == 0).all()
    scanate = session.query(table).filter(table.columns.status == 1).all()
    session.close()

    return render_template('scanare.html', scanate=scanate, nescanate=nescanate, fila=fila, mesaj=mesaj)

@app.route('/nir_search', methods=['GET', 'POST'])
def nir_search():
    mesaj = None
    numar_nir = None
    if request.method == 'POST':
        numar_nir = 'NIR_' + request.form['numar_nir']
        tables = get_tables_from_db()
        if numar_nir in tables:
            mesaj = 'S-a gasit NIR-ul {}.'.format(request.form['numar_nir'])
        else:
            mesaj = 'NIR-ul cu numarul {} nu se afla in baza de date.'.format(request.form['numar_nir'])
            numar_nir = None
    return render_template('nir_search.html', numar_nir=numar_nir, mesaj=mesaj)


if __name__ == '__main__':
    app.run()
