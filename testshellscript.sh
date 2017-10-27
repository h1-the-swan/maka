for (( i = 1900; i < 1920; i++ )); do
	echo "running with arg $i"
	nohup python testpy.py $i >& testpylog/testpylog$i.log &
	if [[ $(($i % 5)) == 0 ]]; then
		echo "waiting for jobs to finish before continuing"
		wait
	fi
done
wait
