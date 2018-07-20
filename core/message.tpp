template<class T>
typename vector<pair<history_t, T>>::iterator merge_hisotry(vector<pair<history_t, T>>& vec, history_t& his, int branch_key){
	if(branch_key != -1){
		auto his_itr = find_if( his.begin(), his.end(),
			[&branch_key](const pair<int, value_t>& element){ return element.first == branch_key;});
		if(his_itr != his.end()){
			his.erase(his_itr + 1, his.end());
		}
	}else{
		his.clear();
	}

	return  find_if( vec.begin(), vec.end(), [&his](const pair<history_t, T>& element){ return element.first == his;});
}

template<class T1, class T2>
size_t MemSize(pair<T1, T2> p)
{
	size_t s = MemSize(p.first);
	s += MemSize(p.second);
	return s;
}

template<class T>
size_t MemSize(vector<T> data)
{
	size_t s = sizeof(size_t);
	for (auto t : data){
		s += MemSize(t);
	}
	return s;
}
