from requests import Session
from requests.exceptions import HTTPError
from argparse import ArgumentParser, FileType
import logging
from logging.handlers import RotatingFileHandler
from pandas import read_json
from urllib.parse import urljoin
import PySimpleGUI as sg
import tempfile
import sys
from datetime import datetime
import json
import re

sg.theme('Dark Blue 3')

log_file = tempfile.gettempdir() + '/busca_retrabalho.log'

formatter = logging.Formatter('%(asctime)s,%(msecs)d | %(name)s | %(levelname)s | %(message)s')

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

file_log_handler = RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=2)
file_log_handler.setLevel(logging.DEBUG)
file_log_handler.setFormatter(formatter)

stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(logging.INFO)
stdout_handler.setFormatter(formatter)

logger.addHandler(file_log_handler)
logger.addHandler(stdout_handler)

parser = ArgumentParser(
    prog='busca_retrabalho',
    description='Busca os retrabalhos lançados no sistema GTRP'
)

parser.add_argument(
    '--host',
    type=str,
    help='Endereço IP da API Ex.: http://localhost:6543/',
    required=True
)

parser.add_argument(
    '-u', '--user',
    type=str,
    help='Usuario que será utilizado para acessar a API',
    required=True
)

parser.add_argument(
    '-p', '--password',
    type=str,
    help='Senha do usuário',
    required=True
)

parser.add_argument(
    '--data-inicio',
    type=datetime.fromisoformat,
    help='Data inicial da busca',
    required=True
)

parser.add_argument(
    '--data-fim',
    type=datetime.fromisoformat,
    help='Data final da busca',
    required=True
)

parser.add_argument(
    '-f', '--file-path',
    type=FileType('w'),
    help='Arquivo csv que será salvo',
    required=True
)

parser.add_argument(
    '--sep',
    type=str,
    help='Separador de campos do arquivo csv',
    default=','
)

parser.add_argument(
    '--regex-id-unico',
    type=str,
    help='Regex que retorno id único no codigo',
    default='#(.*)'
)

parser.add_argument(
    '--regex-id-ordem',
    type=str,
    help='Regex que retorno id ordem no codigo',
    default='(.*)#'
)

urgente_group = parser.add_mutually_exclusive_group()

urgente_group.add_argument(
    '--urgente',
    help='Se especificado, retornará somente retrabalhos urgentes',
    action='store_true'
)

urgente_group.add_argument(
    '--nao-urgente',
    help='Se especificado, retornará somente retrabalhos NÃO urgentes',
    action='store_true'
)

args = parser.parse_args()

logger.debug('Argumentos: %s', args)


URL_RETRABALHO = urljoin(args.host, 'retrabalho')

logger.debug('URL_RETRABALHO: %s', URL_RETRABALHO)

# Cria sessão da API e coleta o token que é utilizado nas futuras requisições
s = Session()

def get_auth_key():

    try:

        login = s.post(
            url = urljoin(args.host, 'auth/login'),
            json= {
                "user":     args.user,
                "password": args.password
            }
        )

        login.raise_for_status()

        logger.info('Sucesso no Login')

        return login.json().get('retorno').get('key')

    except Exception as e:

        logger.exception('')

        try:
            mensagem = e.response.json().get('mensagem')
        except:
            mensagem = 'A API não enviou uma mensagem de erro, verifique os logs.'

        sg.Popup(
            'Não foi possível se conectar a API',
            mensagem,
            f'Log completo em: {log_file}',
            title='Erro ao conectar a API',
            button_type=sg.POPUP_BUTTONS_OK
        )

        raise SystemExit


def main():

    s.headers['Authorization'] = f"Bearer {get_auth_key()}"

    if args.urgente:
        urgente = '1'
    elif args.nao_urgente:
        urgente = '0'
    else:
        urgente = None

    parametros_retrabalho = {
        "data_inicio": args.data_inicio,
        "data_fim":    args.data_fim,
        "urgente":     urgente,
        "tipo":        "SUCATA",
        "inativo":     False,
    }

    res = s.get(
        url=urljoin(args.host, URL_RETRABALHO),
        params=parametros_retrabalho
    )

    try:
        res.raise_for_status()
    except HTTPError as e:

        logger.exception('')

        try:
            mensagem = e.response.json().get('mensagem')
        except:
            mensagem = 'A API não enviou uma mensagem de erro, verifique os logs.'

        logger.error(mensagem)

        response = sg.Popup(
                        'Ocorreu um erro ao buscar retrabalhos com esse parâmetros:',
                        parametros_retrabalho,
                        f'Log completo em: {log_file}',
                        title='Erro ao buscar retrabalhos',
                        button_type=sg.POPUP_BUTTONS_OK
                    )
        
        raise SystemExit
    else:
        logger.debug(f'ok')

    retrabalhos = res.json().get('retorno')

    if not retrabalhos:
        sg.Popup(
                'Não foram encontrados retrabalhos com esse parâmetros:',
                parametros_retrabalho,
                f'Log completo em: {log_file}',
                title='Retrabalho não encontrado',
                button_type=sg.POPUP_BUTTONS_OK
            )
        
        raise SystemExit

    retrabalhos_formatados = []
    
    for i, retrabalho in enumerate(retrabalhos):

        if not sg.one_line_progress_meter(
                            'Processando retrabalhos',
                            i, len(retrabalhos),
                            orientation='h'):
            break

        try:
            id_ordem = re.findall(args.regex_id_ordem, retrabalho['id_ordem'])[0]
        except IndexError:
            id_ordem = retrabalho['id_ordem']

        try:
            id_unico = re.findall(args.regex_id_unico, retrabalho['id_ordem'])[0]
        except IndexError:
            id_unico = ''

        
        try:
            id_ordem_sem_ord = int(id_ordem[3:])
        except ValueError:
            id_ordem_sem_ord = None

        desc_mp: str     = None
        material_mp: str = None

        borda_comp_1: bool = None
        borda_comp_2: bool = None
        borda_larg_1: bool = None
        borda_larg_2: bool = None
        borda_text: str    = None
        tem_furacao: bool  = None

        if id_ordem_sem_ord:

            # Busca o material da Ordem
            try:
                res_ord_mat = s.get(url=urljoin(args.host, f'/focco/ordem/{id_ordem_sem_ord}/material'),)
                res_ord_mat.raise_for_status()
            except HTTPError:
                sg.Popup(
                    f'Houve um erro ao buscar na FOCCO o material da ordem "{id_ordem_sem_ord}"',
                    'Essa ordem irá ser inserida no csv com as informações de material faltando.',
                    title='Erro ao buscar material',
                    button_type=sg.POPUP_BUTTONS_OK
                )
            else:
                desc_mp     = res_ord_mat.json()['retorno']['desc_mp']
                material_mp = res_ord_mat.json()['retorno']['material_mp']

            # Busca o roteiro da ordem
            try:
                res_roteiro = s.get(
                    url=urljoin(args.host, f'/cliente/roteiro'),
                    params={
                        'id_ordem': id_ordem_sem_ord
                    }
                )
                res_roteiro.raise_for_status()
            except HTTPError:
                sg.Popup(
                    f'Houve um erro ao buscar no MES o roteiro da ordem "{id_ordem_sem_ord}"',
                    'Essa ordem irá ser inserida no csv sem as informações de borda e furação.',
                    title='Erro ao buscar roteiro',
                    button_type=sg.POPUP_BUTTONS_OK
                )
            else:
                roteiro: list = res_roteiro.json()['retorno']


                borda_comp_1 = any([operacao.get('codigo_operacao') in ('5', '31') for operacao in roteiro])

                borda_comp_2 = any([operacao.get('codigo_operacao') in ('6', '32') for operacao in roteiro])

                borda_larg_1 = any([operacao.get('codigo_operacao') in ('7', '33') for operacao in roteiro])

                borda_larg_2 = any([operacao.get('codigo_operacao') in ('8', '34') for operacao in roteiro])

                borda_text = f'{sum([borda_larg_1, borda_larg_2])},{sum([borda_comp_1, borda_comp_2])},BORDA'


                tem_furacao = any([operacao.get('codigo_operacao') in ('9', '11', '13', '37') for operacao in roteiro])


        retrabalho_formatado = {
            'PLANO':         retrabalho['lote'],
            'DESC MP':       desc_mp,
            'COD PRODUTO':   retrabalho['codigo'],
            'PRODUTO':       retrabalho['descricao'],
            'REFERENCIA':    '',
            'MATERIAL':      retrabalho['mascara'],
            'MATERIAL MP':   material_mp,
            'LARGURA':       retrabalho['largura'],
            'LARG':          retrabalho['largura'],
            'COMPRIMENTO':   retrabalho['comprimento'],
            'COMP':          retrabalho['comprimento'],
            'QTDE PLC':      retrabalho['qtd'],
            'ORDEM':         retrabalho['num_ordem'], 
            'COD BARRA':     id_ordem,
            'ESPESSURA':     retrabalho['espessura'],
            'VEIO':          '',
            'ID ORD PLANO':  '',
            'DT LOTE':       retrabalho['data'],
            'NUM LOTE':      retrabalho['lote'],
            'PAP PLAST':     '',
            'COD MP':        '',
            'MASC ID MP':    '',
            'BORDA':         borda_text or '',
            'FURACAO':       'SIM' if tem_furacao else '',
            'COD BARRA ORD': id_unico,
            'QTDE ORD':      '',
            'G TOTAL GERAL': ''
        }

        retrabalhos_formatados.append(retrabalho_formatado)

    sg.one_line_progress_meter_cancel()

    df = read_json(json.dumps(retrabalhos_formatados))

    df.to_csv(args.file_path, encoding='utf-8', index=False, sep=args.sep, lineterminator='\n')

    sg.Popup(
        'Envio finalizado!',
        f'Lista de retrabalhos salva em: {args.file_path.name}',
        f'Log completo em: {log_file}',
        title='Envio finalizado',
        button_type=sg.POPUP_BUTTONS_OK
    )

if __name__ == '__main__':
    
    try:
        main()
    except Exception as e:
    
        logger.exception('')

        sg.Popup(
            'Ocorreu um erro não esperado',
            str(e),
            f'Log completo em: {log_file}',
            title='Erro não esperado',
            button_type=sg.POPUP_BUTTONS_OK
        )

        raise SystemExit