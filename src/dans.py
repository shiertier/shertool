from pymongo import MongoClient
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from huggingface_hub.file_download import http_get, hf_hub_url
import os
import json
from tqdm import tqdm

class _Dans():
    def __init__(self):
        self.db_name = 'dan'
        self.uri = {1: 'jiqehmc.uvn7wos', 2: 'r6x57z2.b6l2eyf', 3: 'yuwr7js.zo1zwps', 4: 't6qkwkx.i2wiwoc', 5: '4tazvoi.zzfzm0p', 6: 'cq0gmgu.shrzmq7', 7: 'ihsiyfh.o4oifyo', 8: '3fhtk9y.vzcnntx', 9: 'gifoxwm.sskvpd1', 10: 'meshk5f.zixqgc0', 11: '2zefvgi.bdpicnl', 12: 'ngzgp3x.rlapg6v', 13: 'dkxdusd.zrwtabw', 14: 'vqktfmc.njcteit', 15: 'ealxvkx.ohxomuy', 16: 'szrdgpb.numiong', 17: 'gaaijrd.zq4sotr', 18: 'k82wpv7.llgjizk', 19: 't86uy7t.t2zzrgg', 20: 'h4zgq65.bjrurhl', 21: '6nbko5e.hystrni', 22: 'csrl5re.y6wzxl1', 23: 'jv4ouud.lzkt6in', 24: 'xk7j0mm.4nkvjtq', 25: 'faeq8hx.yuqhhku', 26: '5g2p6g0.5hqmmjx', 27: 'cxf1q45.i5alrhw', 28: 'rkqwc9u.rva1b3c', 29: 'cevdc5h.blofuyb', 30: 'ygguit2.kecmcdx', 31: 'fmelnq3.5z5xdzv', 32: 'dqktzt7.h4dai0c', 33: '2ksgddy.qurnvkq', 34: 'rjxilcc.mkjwmfd', 35: '81zqv1t.1fxho8g', 36: 'dxyrgls.41pce2a', 37: 'hdomtjq.czxy6kf', 38: 'yosa0zv.gv4dtjx', 39: 'lcqbzx1.d0lhgye', 40: 'tbq3ore.fiolllc', 41: '0c40b0y.spgre5u', 42: 'hc7is2w.jgt6t7k', 43: 'kr4vsk6.78nntpx', 44: 'ru2a2my.kruxicb', 45: 'rqf5k6i.mj3otqa', 46: 'jryb6vk.tqtunzh', 47: 'fpqcmgf.umhtaty', 48: 'dhhqbzw.13uw47s', 49: 'rdkbkcr.cshwlqi', 50: 'ldigzge.lreh2eb', 51: 'fcby49h.9k8we21', 52: 'ffg2gtp.gxs4fpu', 53: 'ypfnhkd.5xxeyyq', 54: 'jmy3sov.vmv4d5o', 55: 'c2c32io.fafrswm', 56: 'pdlvhe2.nrbstuq', 57: 'hov6v6c.tmbu8ge', 58: '9kd2d6q.tibh7vc', 59: 'gnszfij.0edndox', 60: 'nda9tek.jzrz4fi', 61: '1bnnmmu.n2snbwq', 62: '8rlp0bo.wibmmva', 63: 'xbeeaon.s3zdxtd', 64: 'pqnmpkm.cc7afxz', 65: 'hgqqcuc.wyqnbgb', 66: 'a5huymj.j8kegb6', 67: 'bcjivdo.3fqdkea', 68: 'oalruqg.2lhaztd', 69: 'b0t5wov.7olzgwi', 70: 'exctnsd.nzsjtzx', 71: 'o148ywo.ckvp3fx', 72: 'm7xqyfa.btvdc0i', 73: 'avmwioq.akzhdwe', 74: '2pdswzk.qbgafhx', 75: 'bj1b0jk.qhq0xaj', 76: 'tk6q6ae.zzdnn0u', 77: 'a11zlw8.oeezui6', 78: 'upjjhsl.ie9uvrj', 79: '4oshoy3.ho0ijpp', 80: 'ywrgqq3.ma2gno0'}
        self.db = {}
        self.clients = {}
        self._get_clients()
        self._login()
        self._artists_id = self._get_tags_dict('_artists.json')
        self._characters_id = self._get_tags_dict('_characters.json')
        self._general_id = self._get_tags_dict('_general.json')
        self._series_id = self._get_tags_dict('_series.json')
        self._artists = self.invert_dict(self._artists_id)
        self._characters = self.invert_dict(self._characters_id)
        self._general = self.invert_dict(self._general_id)
        self._series = self.invert_dict(self._series_id)

    def _list_to_dict(self,_list,key_name='key',value_name='_id'):
        '''
        工具： 列表 -->> 字典
        '''
        _dict = {}
        for i in _list:
            _dict[i[key_name]] = i[value_name]
        return _dict

    def invert_dict(self,_dict):
        '''
        工具： 字典 -->> 反转字典
        '''
        return {v: k for k, v in _dict.items()}

    def _get_tags_dict(self,json_path):
        '''
        工具： 读取基本设置
        '''
        with open(json_path, 'r', encoding='utf-8') as f:
            _list = json.load(f)
        return self._list_to_dict(_list)

    def _get_clients(self):
        for key,uri in self.uri.items():
            # ac-o8xtffk-shard-00-01.pamdkud.mongodb.net
            uri1 = uri.split('.')[0]
            uri2 = uri.split('.')[1]
            self.clients[key] = MongoClient(f"mongodb://shiertier:jkjjjkjjk1@ac-{uri1}-shard-00-01.{uri2}.mongodb.net:27017/?tls=true")

    def _login(self):
        for key,client in self.clients.items():
            exec(f'self.pics{key} = client[self.db_name]["pics"]')

    def _find_one(self, collection, query, sort=None):
        return collection.find_one(query)

    def _find(self, collection, query, sort=None, limit=None):
        if sort is not None and limit is not None:
            return list(collection.find(query).sort(sort).limit(limit))
        elif sort is not None:
            return list(collection.find(query).sort(sort))
        elif limit is not None:
            return list(collection.find(query).limit(limit))
        else:
            return list(collection.find(query))

    def find_one(self, query={}):
        results = []
        with ThreadPoolExecutor(max_workers=100) as executor:
            futures = [executor.submit(self._find_one, eval(f'self.pics{key}'), query) for key in self.uri.keys()]
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result is not None:
                        results.append(result)
                except Exception as exc:
                    print(f'    error | {exc}')
        if len(results) == 0:
            result = None
        else:
            result = results[0]
        return result

    def find_one_random(self, query={}):
        thekey = list(self.uri.keys())
        random.shuffle(thekey)
        for key in thekey:
            count = eval(f'self.pics{key}.count_documents(query)')
            if count:
                pipeline = [
                    {'$match': query},
                    {'$sample': {'size': 1}}
                ]
                result = eval(f'self.pics{key}.aggregate(pipeline).next()')
                if result:
                    return result
        return None


    def find(self, query={}, sort=None, limit=None):
        print(f'searching {query} ...')
        results = []
        with ThreadPoolExecutor(max_workers=100) as executor:
            if sort is not None and limit is not None:
                futures = [executor.submit(self._find, eval(f'self.pics{key}'), query, sort, limit) for key in self.uri.keys()]
            elif sort is not None:
                futures = [executor.submit(self._find, eval(f'self.pics{key}'), query, sort) for key in self.uri.keys()]
            elif limit is not None:
                futures = [executor.submit(self._find, eval(f'self.pics{key}'), query, limit=limit) for key in self.uri.keys()]
            else:
                futures = [executor.submit(self._find, eval(f'self.pics{key}'), query) for key in self.uri.keys()]
        for future in as_completed(futures):
            try:
                result = future.result()
                for r in result:
                    results.append(r)  # 将结果添加到列表中
            except Exception as exc:
                print(f'    error | {exc}')
        if len(results) == 0:
            results = None
        if sort is not None:
            sort1, sort2 = list(sort.items())[0]  # sort1决定排序的字段，sort2决定排序的方式（1为正序，-1为倒序）
            results.sort(key=lambda x: x[sort1], reverse=False if sort2 == 1 else True)
        if limit is not None:
            results = results[:limit]
        return results  # 返回所有查询结果的列表

    def find_last_one(self):
        return self.find({},sort={'_id':-1},limit=1)

    def test_link(self):
        for key in self.uri.keys():
            print(key)
            result = eval(f'self.pics{key}.find_one()')

    def _count(self, collection, query):
        return collection.count_documents(query)

    def count_doc(self,query={}):
        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = [executor.submit(self._count, eval(f'self.pics{key}'), query) for key in self.uri.keys()]
            count = 0
            for future in as_completed(futures):
                try:
                    result = future.result()
                    count += result
                except Exception as exc:
                    print(f'    error | {exc}')
        return count  # 返回所有查询结果的列表

    def _convert_num_to_tag(self,_dict,key,tag_dict):
        '''
        工具： nums -->> tags
        '''
        if _dict:
            try:
                _nums = _dict[key]
                _tags = []
                for i in _nums:
                    try:
                        _tags.append(tag_dict[i])
                    except:
                        continue
                _dict[key] = _tags
            except Exception as e:
                #print(e)
                pass
            return _dict

    def _convert_tags(self,_dict):
        _dict = self._convert_num_to_tag(_dict,"artist",self._artists_id)
        _dict = self._convert_num_to_tag(_dict,"character",self._characters_id)
        _dict = self._convert_num_to_tag(_dict,"general",self._general_id)
        _dict = self._convert_num_to_tag(_dict,"series",self._series_id)
        return _dict

    def convert_tags(self,_dict):
        # 检查_dict是列表还是字典
        if isinstance(_dict,dict):
            return self._convert_tags(_dict)
        if isinstance(_dict,list):
            return [self._convert_tags(_d) for _d in _dict ]

    def search_artist(self, artist_name,sort=None,limit=None):
        try:
            artist_id = dans._artists[artist_name]
        except KeyError:
            print(f'    error | {artist_name} not found')
            return
        artist = dans.find({'artist':{'$in':[artist_id]}},sort=sort,limit=limit)
        print(f'     result | {len(artist)}')
        self.image_to_download = self.convert_tags(artist)
        return self.convert_tags(artist)

    def search_character(self, character_name, remove_solo=True,sort=None,limit=None):
        try:
            character_id = dans._characters[character_name]
        except KeyError:
            print(f'    error | {character_name} not found')
            return
        character = dans.find({'character':{'$in':[character_id]}},sort=sort,limit=limit)
        print(f'     result | {len(character)}')
        character = self.convert_tags(character)
        if remove_solo:
            character =  self.del_key_without_value(character,'general','solo')
        self.image_to_download = self.convert_tags(character)
        return character

    def search_general(self, general_name, remove_solo=True,sort=None,limit=None):
        try:
            general_id = dans._general[general_name]
        except KeyError:
            print(f'    error | {general_name} not found')
            return
        general = dans.find({'general':{'$in':[general_id]}},sort=sort,limit=limit)
        print(f'     result | {len(general)}')
        general = self.convert_tags(general)
        if remove_solo:
            general=self.del_key_without_value(general,'general','solo')
        self.image_to_download = self.convert_tags(general)
        return general

    def search_series(self, series_name,sort=None,limit=None):
        try:
            series_id = dans._series[series_name]
        except KeyError:
            print(f'    error | {series_name} not found')
            return
        series = dans.find({'series':{'$in':[series_id]}},sort=sort,limit=limit)
        print(f'     result | {len(series)}')
        self.image_to_download = self.convert_tags(series)
        return self.convert_tags(series)

    def del_key_without_value(self, list, key, value):
        new_list = []
        for _dict in list:
            if value in _dict[key]:
                new_list.append(_dict)
        print(f'     result | orignal: {len(list)}, after: {len(new_list)}')
        return new_list

    def hf_download_pic(self,id,offset,size,file_path):
        def pad_num(num, length=4):
            return str(num).zfill(length)
        repo_id = 'shiertier/12TPICS'
        tar_path = f'danbooru/webp_2048/{pad_num(id//1000000, 2)}/{pad_num(id//10000, 4)}.tar'
        url_to_download = hf_hub_url(repo_id, tar_path, repo_type='dataset', revision='main', endpoint=None)
        headers = {'Range': f'bytes={offset}-{offset + size - 1}'}
        if file_path:
            os.makedirs(file_path, exist_ok=True)
        pic_path = os.path.join(file_path,f"{id}.webp")
        if os.path.exists(pic_path):
            with open(pic_path, 'rb') as f:
                f.seek(0, 2)
                if f.tell() == size:
                    return
                else:
                    os.remove(pic_path)
        try:
            with open(pic_path, 'wb') as f:
                if size > 0:
                    http_get(
                        url_to_download,
                        f,
                        proxies=None,
                        resume_size=0,
                        headers=headers,
                        expected_size=size,
                        displayed_filename=f"{id}.webp",
                    )

        except Exception as e:
            print(e)
            if os.path.exists(pic_path):
                os.remove(pic_path)
            raise

    def download_pic(self,p,file_path='.'):
        try:
            try:
                id = p['_id']
                offset = p['offset']
                size = p['size']
            except KeyError:
                return 1
            self.hf_download_pic(id,offset,size,file_path)
            return 1
        except Exception as e:
            return 2

    def download_pics(self, file_path='.', bs=16):
        success_count = 0
        failure_count = 0
        with ThreadPoolExecutor(max_workers=bs) as executor:
            futures = [executor.submit(self.download_pic, p, file_path) for p in self.image_to_download]
            for future in tqdm(as_completed(futures), total=len(self.image_to_download), desc="Downloading"):
                result = future.result()
                if result == 1:
                    success_count += 1
                elif result == 2:
                    failure_count += 1
                # 打印进度和结果
                tqdm.write(f"Downloaded {success_count} images, failed {failure_count} images.")
