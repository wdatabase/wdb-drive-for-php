<?php
namespace Wdb\Drive;

class WdbDrive {
    private $host = "http://127.0.0.1:8000";
    private $key = "key";

    public function set_api($host, $key) {
        $this->host = $host;
        $this->key = $key;
    }

    public function CreateObj($key, $data, $categories): ApiRsp {
        $tm = time();
        $sg = $this->sign($this->key.$tm.$key.$data);
        return $this->post_data('/wdb/api/create', json_encode([
            'key' => $key,
            'categories' => $categories,
            'content' => $data,
            'time' => $tm,
            'sg' => $sg,
        ]));
    }

    public function UpdateObj($key, $data): ApiRsp {
        $tm = time();
        $sg = $this->sign($this->key.$tm.$key.$data);
        return $this->post_data('/wdb/api/update', json_encode([
            'key' => $key,
            'content' => $data,
            'time' => $tm,
            'sg' => $sg,
        ]));
    }

    public function GetObj($key): ApiRsp {
        $tm = time();
        $sg = $this->sign($this->key.$tm.$key);
        return $this->get_data('/wdb/api/get?key='.$key.'&time='.$tm.'&sg='.$sg);
    }

    public function DelObj($key): ApiRsp {
        $tm = time();
        $sg = $this->sign($this->key.$tm.$key);
        return $this->get_data('/wdb/api/del?key='.$key.'&time='.$tm.'&sg='.$sg);
    }

    public function ListObj($category, $offset, $limit, $order): ListRsp {
        $tm = time();
        $sg = $this->sign($this->key.$category.$tm);
        $listRsp = $this->get_data('/wdb/api/list?category='.$category.'&offset='.$offset.'&limit='.$limit.'&order='.$order.'&time='.$tm.'&sg='.$sg);
        $rsp = new ListRsp();
        if($listRsp->code == 200){
            $listObj = json_decode($listRsp->data);
            $rsp->total = $listObj->total;
            $rsp->list =  $listObj->list;
            return $rsp;
        } else {
            $rsp->code = 500;
            $rsp->msg = $listRsp->msg;
            return $rsp;
        }
    }

    public function TransBegin($lock_ids): ApiRsp {
        $tm = time();
        $sg = $this->sign($this->key.$tm);
        return $this->post_data('/wdb/api/trans/begin', json_encode([
            'keys' => $lock_ids,
            'time' => $tm,
            'sg' => $sg,
        ]));
    }

    public function TransCreateObj($tsid, $key, $data, $categories): ApiRsp {
        $tm = time();
        $sg = $this->sign($this->key.$tm.$key.$data.$tsid);
        return $this->post_data('/wdb/api/trans/create', json_encode([
            'tsid' => $tsid,
            'key' => $key,
            'categories' => $categories,
            'content' => $data,
            'time' => $tm,
            'sg' => $sg,
        ]));
    }

    public function TransUpdateObj($tsid, $key, $data): ApiRsp {
        $tm = time();
        $sg = $this->sign($this->key.$tm.$key.$data.$tsid);
        return $this->post_data('/wdb/api/trans/update', json_encode([
            'tsid' => $tsid,
            'key' => $key,
            'content' => $data,
            'time' => $tm,
            'sg' => $sg,
        ]));
    }

    public function TransGet($tsid, $key): ApiRsp {
        $tm = time();
        $sg = $this->sign($this->key.$tm.$key.$tsid);
        return $this->get_data('/wdb/api/trans/get?tsid='.$tsid.'&key='.$key.'&time='.$tm.'&sg='.$sg);
    }

    public function TransDelObj($tsid, $key): ApiRsp {
        $tm = time();
        $sg = $this->sign($this->key.$tm.$key.$tsid);
        return $this->get_data('/wdb/api/trans/del?tsid='.$tsid.'&key='.$key.'&time='.$tm.'&sg='.$sg);
    }

    public function TransCommit($tsid): ApiRsp {
        $tm = time();
        $sg = $this->sign($this->key.$tm.$tsid);
        return $this->post_data('/wdb/api/trans/commit', json_encode([
            'tsid' => $tsid,
            'time' => $tm,
            'sg' => $sg,
        ]));
    }

    public function TransRollBack($tsid): ApiRsp {
        $tm = time();
        $sg = $this->sign($this->key.$tm.$tsid);
        return $this->post_data('/wdb/api/trans/roll_back', json_encode([
            'tsid' => $tsid,
            'time' => $tm,
            'sg' => $sg,
        ]));
    }

    public function CreateIndex($indexkeys, $key, $indexraw): ApiRsp {
        $tm = time();
        $sg = $this->sign($this->key.$tm.$key);
        return $this->post_data('/wdb/api/index/create', json_encode([
            'indexkey' => $indexkeys,
            'key' => $key,
            'indexraw'=> $indexraw,
            'time' => $tm,
            'sg' => $sg,
        ]));
    }

    public function UpdateIndex($oindexkeys, $cindexkeys, $key, $indexraw): ApiRsp {
        $tm = time();
        $sg = $this->sign($this->key.$tm.$key);
        return $this->post_data('/wdb/api/index/update', json_encode([
            'oindexkey' => $oindexkeys,
            'cindexkey' => $cindexkeys,
            'key' => $key,
            'indexraw'=> $indexraw,
            'time' => $tm,
            'sg' => $sg,
        ]));
    }

    public function DelIndex($indexkeys, $key): ApiRsp {
        $tm = time();
        $sg = $this->sign($this->key.$tm.$key);
        return $this->post_data('/wdb/api/index/del', json_encode([
            'indexkey' => $indexkeys,
            'key' => $key,
            'time' => $tm,
            'sg' => $sg,
        ]));
    }

    public function ListIndex($indexkey, $condition, $offset, $limit, $order): ListRsp {
        $tm = time();
        $sg = $this->sign($this->key.$indexkey.$tm);
        $listRsp = $this->post_data('/wdb/api/index/list', json_encode([
            'indexkey' => $indexkey,
            'condition' => $condition,
            'offset' => $offset,
            'limit' => $limit,
            'order' => $order,
            'time' => $tm,
            'sg' => $sg,
        ]));
        $rsp = new ListRsp();
        if($listRsp->code == 200){
            $listObj = json_decode($listRsp->data);
            $rsp->total = $listObj->total;
            $rsp->list =  $listObj->list;
            return $rsp;
        } else {
            $rsp->code = 500;
            $rsp->msg = $listRsp->msg;
            return $rsp;
        }
    }

    public function CreateRawData($key, $raw, $categories): ApiRsp {
        $content = base64_encode($raw);
        $tm = time();
        $sg = $this->sign($this->key.$tm.$key.$content);
        return $this->post_data('/wdb/api/create_raw', json_encode([
            'key' => $key,
            'categories' => $categories,
            'content' => $content,
            'time' => $tm,
            'sg' => $sg,
        ]));
    }

    public function GetRawData($key): RawRsp {
        $tm = time();
        $sg = $this->sign($this->key.$tm.$key);
        $rawRsp = $this->get_data('/wdb/api/get_raw?key='.$key.'&time='.$tm.'&sg='.$sg);
        $rsp = new RawRsp();
        if(isset($rawRsp->code) && $rawRsp->code == 200) {
            $data = base64_decode($rawRsp->data);
            $rsp->raw = $data;
            return $rsp;   
        } else {
            $rsp->code = 500;
            $rsp->msg = $rawRsp->msg;
            return $rsp;
        }
    }

    public function GetRangeData($key, $offset, $limit): RangeRsp {
        $tm = time();
        $sg = $this->sign($this->key.$tm.$key);
        $rangeRsp = $this->get_data('/wdb/api/get_range?key='.$key.'&offset='.$offset.'&limit='.$limit.'&time='.$tm.'&sg='.$sg);
        $rsp = new RangeRsp();
        if(isset($rangeRsp->code) && $rangeRsp->code == 200) {
            $range_data = json_decode($rangeRsp->data);
            $raw = base64_decode($range_data->data);

            $rsp->all_size = $range_data->all_size;
            $rsp->raw = $raw;

            return $rsp;
        } else {
            $rsp->code = 500;
            $rsp->msg = $rangeRsp->msg;
            return $rsp;
        }
    }

    public function UploadByPath($path, $key, $categories): ApiRsp {
        $tm = time();
        $sg = $this->sign($this->key.$tm.$key);
        return $this->post_data('/wdb/api/upload', json_encode([
            'path' => $path,
            'key' => $key,
            'categories' => $categories,
            'time' => $tm,
            'sg' => $sg,
        ]));
    }

    public function DownToPath($path, $key): ApiRsp {
        $tm = time();
        $sg = $this->sign($this->key.$tm.$key);
        return $this->post_data('/wdb/api/down', json_encode([
            'path' => $path,
            'key' => $key,
            'time' => $tm,
            'sg' => $sg,
        ]));
    }

    public function sign($text) {
        $re = hash('sha256', $text);
        return $re;
    }

    private function get_data($path): ApiRsp {
        $curl = curl_init();
        curl_setopt($curl, CURLOPT_URL, $this->host.$path);
        curl_setopt($curl, CURLOPT_HEADER, 0);
        curl_setopt($curl, CURLOPT_RETURNTRANSFER, 1);
        $data = curl_exec($curl);
        $error = curl_error($curl);
        curl_close($curl);

        $rsp = new ApiRsp();
        if($error != '') {
            $rsp->code = 500;
            $rsp->msg = $error;
            return $rsp;
        }
        if($data == '') {
            $rsp->code = 500;
            $rsp->msg = 'api bac empty';
            return $rsp;
        }

        $obj = json_decode($data);
        if(!isset($obj->code)){
            $rsp->code = 500;
            $rsp->msg = $data;
            return $rsp;
        }

        $rsp->code = $obj->code;
        $rsp->msg = $obj->msg;
        $rsp->data = $obj->data;

        return $rsp;
    }

    private function post_data($path, $post_data): ApiRsp {
        $curl = curl_init();
        curl_setopt($curl, CURLOPT_URL, $this->host.$path);
        curl_setopt($curl, CURLOPT_HEADER, 0);
        curl_setopt($curl, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($curl, CURLOPT_POST, 1);
        curl_setopt($curl, CURLOPT_POSTFIELDS, $post_data);
        curl_setopt($curl, CURLOPT_HTTPHEADER, [
            'Content-Type: application/json',
            'Content-Length: ' . strlen($post_data)
        ]);
        $data = curl_exec($curl);
        $error = curl_error($curl);
        curl_close($curl);

        $rsp = new ApiRsp();
        if($error != ''){
            $rsp->code = 500;
            $rsp->msg = $error;
            return $rsp;
        }
        if($data == ''){
            $rsp->code = 500;
            $rsp->msg = 'api bac empty';
            return $rsp;
        }

        $obj = json_decode($data);
        if(!isset($obj->code)){
            $rsp->code = 500;
            $rsp->msg = $data;
            return $rsp;
        }

        $rsp->code = $obj->code;
        $rsp->msg = $obj->msg;
        $rsp->data = $obj->data;

        return $rsp;
    }

}

class ApiRsp{
    public $code = 200;
    public $msg = '';
    public $data = '';
}

class ListRsp {
    public $code = 200;
    public $msg = '';
    public $total = 0;
    public $list = [];
}

class RawRsp {
    public $code = 200;
    public $msg = '';
    public $raw = '';
}

class RangeRsp {
    public $code = 200;
    public $msg = '';
    public $all_size = 0;
    public $raw = '';
}