以下のファイルを準備
- data_generator.py
- Dockerfile
- configmap.yaml
- secret.yaml
- job.yaml
```
oc apply -f configmap.yaml
oc apply -f secret.yaml
```
BuildConfigの作成
```
oc new-build --name data-generator --strategy docker --binary
```
現在のディレクトリからビルド開始
```
oc start-build data-generator --from-dir=. --follow
```
イメージのパスを内部イメージストリームに合わせて更新：
```
oc get is data-generator
```
Jobの実行
```
oc apply -f job.yaml
```
実行状況の確認
Podの状態確認
```
oc get pods
```
ログの確認
```
oc logs $(oc get pods -l job-name=data-generator -o name | head -1) -f
```
