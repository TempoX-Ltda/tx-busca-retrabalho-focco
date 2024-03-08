from dataclasses import dataclass
from enum import Enum
from typing import Optional
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
from datetime import date, datetime
import json

__version__ = '1.0.9'

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
    '-v', '--version',
    action='version',
    version='%(prog)s ' + __version__
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
            },
            timeout=10
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

class TipoRetrabalho(str, Enum):
    APROVEITAMENTO = "APROVEITAMENTO"
    SUCATA = "SUCATA"
    RETRABALHO_DE_BORDA = "RETRABALHO DE BORDA"
    RETRABALHO = "RETRABALHO"
    IMPORTACAO_MANUAL = "IMPORTACAO MANUAL"

@dataclass
class Retrabalho:
    created_on: datetime
    modified_on: datetime
    id: int
    tipo: TipoRetrabalho
    codigo_lote: Optional[int]
    id_ordem: int
    id_unico_peca: Optional[int]
    item_codigo: str
    item_descricao: Optional[str]
    qtd: int
    mm_largura: Optional[int]
    mm_comprimento: Optional[int]
    mm_espessura: Optional[int]
    id_setor: int
    descricao_setor: str
    id_recurso: int
    apelido_recurso: str
    id_usuario: Optional[int]
    responsavel: Optional[str]
    inativo: bool
    urgente: bool
    item_mascara: str
    item_mascara_descricao: Optional[str]
    motivo_retrabalho: str
    id_turno: Optional[int]
    descricao_turno: Optional[str]

@dataclass
class FoccoOrdemMateiralGetReturn:
    desc_mp: str
    material_mp: str
    mascara_material: str

@dataclass
class InfoOrdemFocco:
    Numero: int

def buscaRetrabalhosDoMES(
        created_on_gt: datetime,
        created_on_lt: datetime,
        urgente: Optional[bool],
        inativo: bool
    ):
    retrabalhos: list[Retrabalho] = []
    last_page = 1000

    for i in range(1000):

        if not sg.one_line_progress_meter(
                'Buscando retrabalhos',
                i, last_page,
                orientation='h'):
            break

        params = {
            "created_on_gt": created_on_gt.isoformat(),
            "created_on_lt": created_on_lt.isoformat(),
            "urgente": urgente,
            "inativo": inativo,
            "tipo": "SUCATA",
            "page": i,
            "page_size": 100
        }

        res = s.get(
            url=urljoin(args.host, URL_RETRABALHO),
            params=params
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
                            params,
                            f'Log completo em: {log_file}',
                            title='Erro ao buscar retrabalhos',
                            button_type=sg.POPUP_BUTTONS_OK
                        )
            
            raise SystemExit
        else:
            logger.debug(f'ok')

        result = res.json()

        retrabalho_desta_pagina = [
            Retrabalho(
                apelido_recurso=r['apelido_recurso'],
                created_on=datetime.fromisoformat(r['created_on']),
                descricao_setor=r['descricao_setor'],
                id=r['id'],
                id_ordem=r['id_ordem'],
                id_recurso=r['id_recurso'],
                id_setor=r['id_setor'],
                id_turno=r['id_turno'],
                id_unico_peca=r['id_unico_peca'],
                id_usuario=r['id_usuario'],
                inativo=r['inativo'],
                item_codigo=r['item_codigo'],
                item_descricao=r['item_descricao'],
                item_mascara=r['item_mascara'],
                item_mascara_descricao=r['item_mascara_descricao'],
                mm_comprimento=r['mm_comprimento'],
                mm_espessura=r['mm_espessura'],
                mm_largura=r['mm_largura'],
                modified_on=datetime.fromisoformat(r['modified_on']),
                motivo_retrabalho=r['motivo_retrabalho'],
                qtd=r['qtd'],
                responsavel=r['responsavel'],
                tipo=TipoRetrabalho(r['tipo']),
                urgente=r['urgente'],
                codigo_lote=r['codigo_lote'],
                descricao_turno=r['descricao_turno']
            ) for r in result['retorno']
        ]

        retrabalhos = [*retrabalhos, *retrabalho_desta_pagina]

        last_page = result["metadata"]["last_page"]

        if not retrabalhos or last_page == None or i >= last_page:
            break

    sg.one_line_progress_meter_cancel()

    if not retrabalhos:
        sg.Popup(
                'Não foram encontrados retrabalhos com esse parâmetros:',
                params,
                f'Log completo em: {log_file}',
                title='Retrabalho não encontrado',
                button_type=sg.POPUP_BUTTONS_OK
            )
        
        raise SystemExit
    return retrabalhos

def buscaMaterialFocco(retrabalho: Retrabalho):
    try:
        res_ord_mat = s.get(url=urljoin(args.host, f'/focco/ordem/{retrabalho.id_ordem}/material'),)
        res_ord_mat.raise_for_status()

        retorno = res_ord_mat.json()['retorno']

        return FoccoOrdemMateiralGetReturn(
            desc_mp=retorno['desc_mp'],
            material_mp=retorno['material_mp'],
            mascara_material=retorno['mascara_material']
        )
    except HTTPError:
        sg.Popup(
            f'Houve um erro ao buscar na FOCCO o material da ordem "{retrabalho.id_ordem}"',
            f'Codigo: {retrabalho.item_codigo}',
            f'Descrição: {retrabalho.item_descricao}',
            'Essa ordem irá ser inserida no csv com as informações de material faltando.',
            title='Erro ao buscar material',
            auto_close=True,
            auto_close_duration=5,
            button_type=sg.POPUP_BUTTONS_OK
        )

def buscaRoteiroOrdem(retrabalho: Retrabalho):
    try:
        res_roteiro = s.get(
            url=urljoin(args.host, '/cliente/roteiro'),
            params={
                'id_ordem': retrabalho.id_ordem
            }
        )
        res_roteiro.raise_for_status()

        return res_roteiro.json()['retorno']
    
    except HTTPError:
        sg.Popup(
            f'Houve um erro ao buscar no MES o roteiro da ordem "{retrabalho.id_ordem}"',
            f'Codigo: {retrabalho.item_codigo}',
            f'Descrição: {retrabalho.item_descricao}',
            'Essa ordem irá ser inserida no csv sem as informações de borda e furação.',
            title='Erro ao buscar roteiro',
            auto_close=True,
            auto_close_duration=5,
            button_type=sg.POPUP_BUTTONS_OK
        )

def ordemTemFuracao(roteiro):
    if not roteiro:
        return False

    return any(operacao.get('codigo_operacao') in ('9', '11', '13', '37') for operacao in roteiro)

def textoBorda(roteiro):
    if not roteiro:
        return ''

    borda_comp_1 = any(operacao.get('codigo_operacao') in ('5', '31') for operacao in roteiro)

    borda_comp_2 = any(operacao.get('codigo_operacao') in ('6', '32') for operacao in roteiro)

    borda_larg_1 = any(operacao.get('codigo_operacao') in ('7', '33') for operacao in roteiro)

    borda_larg_2 = any(operacao.get('codigo_operacao') in ('8', '34') for operacao in roteiro)

    return f'{sum([borda_larg_1, borda_larg_2])},{sum([borda_comp_1, borda_comp_2])},BORDA'

def buscaInfoFocco(retrabalho: Retrabalho):
    try:
        res = s.get(url=urljoin(args.host, '/focco/consulta_ordem'),params={'id_ordem': retrabalho.id_ordem})
        res.raise_for_status()

        retorno = res.json()['retorno']

        return InfoOrdemFocco(
            Numero=retorno['Numero']
        )
    except HTTPError:
        sg.Popup(
            f'Houve um erro ao buscar na FOCCO mais informações sobre a ordem "{retrabalho.id_ordem}"',
            f'Codigo: {retrabalho.item_codigo}',
            f'Descrição: {retrabalho.item_descricao}',
            'Essa ordem irá ser inserida no csv sem as informações: NUMERO DA ORDEM.',
            title='Erro ao buscar informações da ordem',
            auto_close=True,
            auto_close_duration=5,
            button_type=sg.POPUP_BUTTONS_OK
        )

def main():

    s.headers['Authorization'] = f"Bearer {get_auth_key()}"

    if args.urgente:
        urgente = True
    elif args.nao_urgente:
        urgente = False
    else:
        urgente = None

    retrabalhos = buscaRetrabalhosDoMES(
        created_on_gt=args.data_inicio,
        created_on_lt=args.data_fim,
        urgente=urgente,
        inativo=False
    )    

    retrabalhos_formatados = []
    
    for i, retrabalho in enumerate(retrabalhos):

        if not sg.one_line_progress_meter(
                            'Processando retrabalhos',
                            i, len(retrabalhos),
                            orientation='h'):
            break

        materialFocco = buscaMaterialFocco(retrabalho)

        roteiro = buscaRoteiroOrdem(retrabalho)

        tem_furacao = ordemTemFuracao(roteiro)

        borda_text = textoBorda(roteiro)

        info_focco = buscaInfoFocco(retrabalho)

        retrabalho_formatado = {
            'PLANO':         retrabalho.codigo_lote or '',
            'DESC MP':       materialFocco.desc_mp if materialFocco is not None else '',
            'COD PRODUTO':   retrabalho.item_codigo,
            'PRODUTO':       retrabalho.item_descricao or '',
            'REFERENCIA':    '',
            'MATERIAL':      materialFocco.mascara_material if materialFocco is not None else '',
            'MATERIAL MP':   materialFocco.material_mp if materialFocco is not None else '',
            'LARGURA':       retrabalho.mm_largura or '',
            'LARG':          retrabalho.mm_largura or '',
            'COMPRIMENTO':   retrabalho.mm_comprimento or '',
            'COMP':          retrabalho.mm_comprimento or '',
            'QTDE PLC':      retrabalho.qtd,
            'ORDEM':         info_focco.Numero if info_focco is not None else '', 
            'COD BARRA':     f'ORD{retrabalho.id_ordem}',
            'ESPESSURA':     retrabalho.mm_espessura or '',
            'VEIO':          '',
            'ID ORD PLANO':  '',
            'DT LOTE':       '',
            'NUM LOTE':      retrabalho.codigo_lote or '',
            'PAP PLAST':     '',
            'COD MP':        '',
            'MASC ID MP':    '',
            'BORDA':         borda_text,
            'FURACAO':       'SIM' if tem_furacao else '',
            'COD BARRA ORD': retrabalho.id_unico_peca or '',
            'QTDE ORD':      '',
            'G TOTAL GERAL': ''
        }

        retrabalhos_formatados.append(retrabalho_formatado)

    sg.one_line_progress_meter_cancel()

    df = read_json(json.dumps(retrabalhos_formatados), dtype=None)

    df.to_csv(args.file_path, encoding='utf-8', index=False, sep=args.sep, lineterminator='\n')

    sg.Popup(
        'Download finalizado!',
        f'Lista de retrabalhos salva em: {args.file_path.name}',
        f'Log completo em: {log_file}',
        title='Download finalizado',
        auto_close=True,
        auto_close_duration=5,
        button_type=sg.POPUP_BUTTONS_OK
    )

if __name__ == '__main__':

    try:
        main()
    except Exception as exc:

        logger.exception('')

        sg.Popup(
            'Ocorreu um erro não esperado',
            str(exc),
            f'Log completo em: {log_file}',
            title='Erro não esperado',
            button_type=sg.POPUP_BUTTONS_OK
        )

        raise SystemExit from exc
