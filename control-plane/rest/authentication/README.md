**WARNING**: These are dummy example RSA keys and should not be used in production.

There are various websites (such as https://russelldavies.github.io/jwk-creator/) which provide the capability of generating the JSON Web Key from the public RSA key.
For convenience the 'jwk' file has already been generated from the provided public key.

# Usage
To try out the dummy JSON Web Key (JWK), execute the following steps from within the nix-shell:
1. Run the deployer without launching the rest service
```bash
./target/debug/deployer start -a "Node, Pool, Volume" --no-rest
```
2. Start the REST service within the nix-shell
```bash
./target/debug/rest --dummy-certificates --jwk "../Mayastor/control-plane/rest/authentication/jwk"
```
2. Set the token value (located in ../Mayastor/control-plane/rest/authentication/token)
```bash
export TOKEN=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJyYW5kb20gc3ViamVjdCIsImNvbXBhbnkiOiJteSBjb21wYW55IiwiZXhwIjoxMDAwMDAwMDAwMH0.GkcWHAJ4-qXihaR2j8ZvJgFB1OPpo9P5PkauTmb4PHvlDTYpDQy_nfTHmZCKHS1WEBtsH-HOXApKf32oJEU0K_2SAO76PVZrqvfMewccny-aB9gyu6WMlgSWK8wvGq4h_t_Ma4KIBlPv5PCQO1fyv9bWM3Y3Lu2rPxvNg0O_V_mfnq_Ynwcy4qhnZmse8pZ9zJJaM5OPv2ucWRPKWNzSX8OOz11MGBcdV5QBM-eBpjeSvejEwQ1xOxfiwZwZosFKjPnwMWn8dirMhMNqyRwWgjmOFU2hpc13Ik2VcSWEKTF4ndoUmMLXmCmQ2pSrn9MihEfkpO_VHx_sRVtmYVe2R4iy7ocul3eG7ZAvRq-_GIqBpwbcdUPANIyEFWUWgiPB5_kFvf4-iIBip7NhZ0_4DVoqukYBM2XodejXY863p2frglljt23EimNoKlrtqyxw1wXcbsYtiqCsd3cFTMUkrVesu9xNQPfpM8so37SmTsrC1nOssGEiADAGowqu5SsS
```
3. Use curl to make a REST request using the above token
```bash
curl -X GET "https://localhost:8080/v0/nodes" -H "accept: application/json" -H "Authorization: Bearer ${TOKEN}" -k
```