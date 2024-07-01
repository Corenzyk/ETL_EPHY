##!/bin/bash



# Fichier d'entrée
input_file="produits_utf8.csv"
# Fichier de sortie
output_file="processed_data.csv"



# Processus 1 : Faire une ligne pour chaque nom commercial

# Étape 1 : Fusionner la 3ème et 4ème colonne
awk -F, 'BEGIN { FS=OFS=";" } { 

    # Si la colonne 4 (celle des seconds noms) n est pas vide
    if ($4!="") {

        # Si il s agit de la ligne de l entete vider la 4eme colonne
        if (NR==1){
            $4=""
        }

        # Sinon fusionner la 3eme et la 4eme en les séparant par un pipe puis vider la 4eme
        else{
            $3 = $3 "|" $4; 
            $4 = ""
        }
    }

    # Afficher chaque ligne
    for (i = 1; i <= NF; i++) {
        printf "%s%s", $i, (i==NF ? "\n" : ";")
    }
}' "$input_file" > "$output_file"

# Etape 1.5 : Supprimer les double ; résultants du merge des nom commerciaux et des seconds noms commerciaux
sed 's/;;/;/' "$output_file" > "$output_file".tmp && mv "$output_file".tmp "$output_file"

# Étape 2 : Supprimer les espaces bordant les pipes
sed -i 's/ *| */|/g' "$output_file"

# Étape 3 : Créer une nouvelle ligne pour chaque nom commercial
awk -F, 'BEGIN{FS=OFS=";"} {

    # Séparer pour chaque pipe dans la colonne des noms
    n=split($3,s,"|"); 

    # Pour chaque split, parcoure le tableau s afin d afficher le nom stocker à s[i] dans une nouvelle ligne
    for (i=1;i<=n;i++) {
        $3=s[i]; print
    }
}' "$output_file" > "$output_file".tmp && mv "$output_file".tmp "$output_file"


# Processus 2 : Faire une ligne pour chaque substance chimique 

# Etape 1 : Créer une nouvelle ligne pour chaque substance chimique 
awk -F, 'BEGIN{FS=OFS=";"} {

    # Séparer pour chaque pipe dans la colonne des substances chimique
    n=split($10,s,"|"); 

    # Pour chaque split, parcoure le tableau s afin d afficher le nom stocker à s[i] dans une nouvelle ligne
    for (i=1;i<=n;i++) {
        $10=s[i]; print
    }
}' "$output_file" > "$output_file".tmp && mv "$output_file".tmp "$output_file" 

# Etape 2 : Ajouter 4 nouvelles colonnes pour chaque ligne correspondant à nom , second nom, concentration, unité
awk -F, 'BEGIN { FS=OFS=";" } {

    # Si il s agit de la premiere ligne, rajouter les entetes
    if (NR==1) { 
        $10=$10";nom;seconds noms;concentration;unite"
    }

    # Sinon, rajouter l espace pour rentrer les informations
    else {
        $10=$10";;;;"
    }
} 1
' "$output_file" > "$output_file".tmp && mv "$output_file".tmp "$output_file" 

# Etape 3 : Séparer les données de la substance dans les nouveaux champs
awk -F, 'BEGIN { FS=OFS=";" } {

    # Si il ne s agit pas de la premiere ligne
    if (NR!=1) {
        col10 = $10
    
        # Séparer la partie du début jusqu à la premiere paranthèse (nom)
        match(col10, /\(/)
        col11 = substr(col10, 1, RSTART-1)
    
        # Séparer la partie de la première paranthèse jusqu à la dernière paranthèse (seconds nom)
        match(col10, /\(.*\)/)
        col12 = substr(col10, RSTART+1, RLENGTH-2)
    
        # Si il y a des paranthese dans la colonne 12 (du a plusieurs noms entre paranthèse), remplacer les paranthèses par des pipes
        gsub(/\)\s*\(/, " | ", col12)

        #
        last_close_paren = RSTART + RLENGTH - 1
        after_paren_part = substr(col10, last_close_paren + 1)

        # Séparer la partie de la dernière paranthèse jusqu au dernier chiffre (concentration)
        match(after_paren_part, /[0-9]+\.[0-9]+/)
        col13 = substr(after_paren_part, RSTART, RLENGTH)
    
        # Séparer la partie du dernier chiffre jusqu à la fin de la ligne (unite)
        match(after_paren_part, / [^ ]+$/)
        col14 = substr(after_paren_part, RSTART+1, RLENGTH-1)
    
        # Affecter les colonnes temporaires aux vraies colonnes de l objet
        $11 = col11
        $12 = col12
        $13 = col13
        $14 = col14
        $10 = ""
    }
    
    # Afficher
    print $0
}
' "$output_file" > "$output_file".tmp && mv "$output_file".tmp "$output_file"



# Processus 3 : Transformation en json 

# Fichier d'entrée
input_file="processed_data.csv"
# Fichier de sortie
output_file="produits_utf8.json"

# Etape 0 : Retirer les ; à la fin de chaque ligne (générait une dernière ligne vide à chaque fois)
sed 's/;$//' "$input_file" > "$input_file".tmp && mv "$input_file".tmp "$input_file"

# Etape 1 : Transformer en json
awk -v OFS=';' '
BEGIN {
    FS = OFS
    print "[" > "'$output_file'"
}

# Extraire la première ligne (ligne des noms)
NR == 1 {
    for (i = 1; i <= NF; i++) {
        headers[i] = $i
    }
}

# Mettre en formes les autres lignes
NR > 1 {
    printf "  {\n" >> "'$output_file'"
    for (i = 1; i <= NF; i++) {
        printf "    \"%s\": \"%s\"", headers[i], $i >> "'$output_file'"
        if (i < NF) {
            printf "," >> "'$output_file'"
        }
        printf "\n" >> "'$output_file'"
    }
    printf "  }" >> "'$output_file'"
    if (NR > 1 && !EOF) {
        printf "," >> "'$output_file'"
    }
    printf "\n" >> "'$output_file'"
}
END {

    print "]" >> "'$output_file'"
}
' $input_file

# Etape 1.5 : Retirer la dernière virgule du fichier (créait un dernier champ vide)
sed -z 's/,\s*\([]}]\)/\1/' "$output_file" > "$output_file".tmp && mv "$output_file".tmp "$output_file"

# Etape 2 : Gestion des substance chimique en objet (ligne 11, 12, 13 et 14 deviennent un objet de la ligne 10) et ajout d'un ligne pour prendre en compte un id
jq 'map(.["Substances actives"] = {
    "id":"",
    "nom": .nom,
    "seconds noms": .["seconds noms"],
    "concentration": .concentration,
    "unite": .unite
} | del(.nom, .["seconds noms"], .concentration, .unite))' "$output_file" > "$output_file".tmp && mv "$output_file".tmp "$output_file"



# Processus 4 : Faire le lien avec les substances actives de la base de données de means

source_file="ppp-means-active-substances.json"

# Etape 1 : Pour chaque produits identique (meme Numero AMM et meme Nom), fusionner les substances dans un seul tableau sans changer les autres lignes
jq 'group_by(.["numero AMM", "nom produit"]) | map({
    "type produit": .[0]["type produit"],
    "numero AMM": .[0]["numero AMM"],
    "nom produit": .[0]["nom produit"],
    "titulaire": .[0]["titulaire"],
    "type commercial": .[0]["type commercial"],
    "gamme usage": .[0]["gamme usage"],
    "mentions autorisees": .[0]["mentions autorisees"],
    "restrictions usage": .[0]["restrictions usage"],
    "restrictions usage libelle": .[0]["restrictions usage libelle"],
    "Substances actives": map(.["Substances actives"]),
    "fonctions": .[0]["fonctions"],
    "formulations": .[0]["formulations"],
    "Etat d’autorisation": .[0]["Etat d’autorisation"],
    "Date de retrait du produit": .[0]["Date de retrait du produit"],
    "Date de première autorisation": .[0]["Date de première autorisation"],
    "Numéro AMM du produit de référence": .[0]["Numéro AMM du produit de référence"],
    "Nom du produit de référence":  .[0]["Nom du produit de référence"]
})' "$output_file" > "$output_file".tmp && mv "$output_file".tmp "$output_file"

# Etape 2 : Trouve le nom des substances dans le fichier de means et associer l'id correspondant (traiter le maximum de cas particuliers)
jq --slurpfile substances "$source_file" '
  . as $input |
  ($substances[0] | map({key: .nomFr, value: .id}) + map({key: .nomEn, value: .id}) | from_entries) as $mapping |
  map(
    .["Substances actives"] |= map(
      .id = (
        $mapping[."nom"] // 
        $mapping[(."nom" | .[:-1])] // 
        $mapping[."nom" + "e"] // 
        $mapping[."nom" + " (NA)"] // 
        $mapping[."nom" + " HCl"] // 
        $mapping["2,4 " + ."nom"] // 
        $mapping[(."nom" | gsub("-"; " "))] // 
        $mapping[(."nom" | gsub(" "; "-"))] // 
        $mapping[(."nom" | gsub("-"; " ")) + " (NA)"] // 
        $mapping[(."nom" | gsub(" "; "-")) + " (NA)"] // 
        $mapping[(."nom" | .[:-1]) + " (NA)"] // 

        $mapping[."seconds noms"] // 
        $mapping[(."seconds noms" | .[:-1])] // 
        $mapping[."seconds noms" + "e"] // 
        $mapping[."seconds noms" + " (NA)"] // 
        $mapping[."seconds noms" + " HCl"] // 
        $mapping["2,4 " + ."seconds noms"] // 
        $mapping[(."seconds noms" | gsub("-"; " "))] // 
        $mapping[(."seconds noms" | gsub(" "; "-"))] // 
        $mapping[(."seconds noms" | gsub("-"; " ")) + " (NA)"] // 
        $mapping[(."seconds noms" | gsub(" "; "-")) + " (NA)"] // 
        $mapping[(."seconds noms" | .[:-1]) + " (NA)"] // 
        
        null
      )
    )
  )
' "$output_file"  > "$output_file".tmp && mv "$output_file".tmp "$output_file"