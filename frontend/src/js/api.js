
function getCrossSection() {
    var values = [];
    fetch('/gsp-regions-lowpoly.json')
        .then(response => response.json())
        .then(data => {
            for (let i = 0; i < data.features.length; i++) {
                let region = data.features[i].properties.GSPs
                values.push({
                    id: region,
                    value: Math.random() * 100
                })
            };
        })
    return values;
};

export { getCrossSection };
