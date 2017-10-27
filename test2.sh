for (( i = 0; i < 10; i++ )); do
    echo $(date)
    echo $(time)
	if [[ $(($i % 5)) == 0 ]]; then
		echo $i
	fi
done
